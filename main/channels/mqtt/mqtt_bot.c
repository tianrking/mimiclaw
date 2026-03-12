#include "mqtt_bot.h"
#include "mimi_config.h"
#include "bus/message_bus.h"
#include "wifi/wifi_manager.h"

#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_log.h"
#include "esp_event.h"
#include "esp_system.h"
#include "mqtt_client.h"
#include "nvs.h"
#include "cJSON.h"
#include "esp_wifi.h"

static const char *TAG = "mqtt";

/* ── Configuration state ────────────────────────────────────── */
static char s_broker_uri[256] = MIMI_SECRET_MQTT_URI;
static char s_client_id[64] = MIMI_SECRET_MQTT_CLIENT_ID;
static char s_username[128] = MIMI_SECRET_MQTT_USERNAME;
static char s_password[128] = MIMI_SECRET_MQTT_PASSWORD;
static char s_subscribe_topic[128] = MIMI_MQTT_DEFAULT_SUB_TOPIC;

static esp_mqtt_client_handle_t s_mqtt_client = NULL;
static bool s_connected = false;
static bool s_enabled = false;

/* ── Message deduplication ──────────────────────────────────── */
#define MQTT_DEDUP_CACHE_SIZE 64

static uint64_t s_seen_msg_keys[MQTT_DEDUP_CACHE_SIZE] = {0};
static size_t s_seen_msg_idx = 0;

static uint64_t fnv1a64(const char *s)
{
    uint64_t h = 1469598103934665603ULL;
    if (!s) return h;
    while (*s) {
        h ^= (unsigned char)(*s++);
        h *= 1099511628211ULL;
    }
    return h;
}

static bool dedup_check_and_record(const char *topic, const char *payload)
{
    /* Combine topic and payload for unique key */
    char combined[320];
    snprintf(combined, sizeof(combined), "%s:%s", topic, payload);
    uint64_t key = fnv1a64(combined);

    for (size_t i = 0; i < MQTT_DEDUP_CACHE_SIZE; i++) {
        if (s_seen_msg_keys[i] == key) return true;
    }
    s_seen_msg_keys[s_seen_msg_idx] = key;
    s_seen_msg_idx = (s_seen_msg_idx + 1) % MQTT_DEDUP_CACHE_SIZE;
    return false;
}

/* ── MQTT Event Handler ─────────────────────────────────────── */
static void mqtt_event_handler(void *handler_args, esp_event_base_t base,
                               int32_t event_id, void *event_data)
{
    (void)base;
    (void)handler_args;

    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)event_data;

    switch (event_id) {
        case MQTT_EVENT_CONNECTED:
            s_connected = true;
            ESP_LOGI(TAG, "MQTT connected to %s", s_broker_uri);

            /* Subscribe to the configured topic pattern */
            if (s_subscribe_topic[0] != '\0') {
                int msg_id = esp_mqtt_client_subscribe(s_mqtt_client, s_subscribe_topic, 1);
                if (msg_id >= 0) {
                    ESP_LOGI(TAG, "Subscribed to topic: %s (msg_id=%d)", s_subscribe_topic, msg_id);
                } else {
                    ESP_LOGW(TAG, "Failed to subscribe to topic: %s", s_subscribe_topic);
                }
            }
            break;

        case MQTT_EVENT_DISCONNECTED:
            s_connected = false;
            ESP_LOGW(TAG, "MQTT disconnected");
            break;

        case MQTT_EVENT_SUBSCRIBED:
            ESP_LOGD(TAG, "MQTT subscribed, msg_id=%d", event->msg_id);
            break;

        case MQTT_EVENT_UNSUBSCRIBED:
            ESP_LOGD(TAG, "MQTT unsubscribed, msg_id=%d", event->msg_id);
            break;

        case MQTT_EVENT_PUBLISHED:
            ESP_LOGD(TAG, "MQTT published, msg_id=%d", event->msg_id);
            break;

        case MQTT_EVENT_DATA:
            {
                /* Extract topic and data */
                char *topic = strndup(event->topic, event->topic_len);
                char *data = strndup(event->data, event->data_len);

                if (!topic || !data) {
                    free(topic);
                    free(data);
                    break;
                }

                ESP_LOGI(TAG, "Received on topic [%.*s]: %.60s%s",
                         event->topic_len, event->topic,
                         data,
                         event->data_len > 60 ? "..." : "");

                /* Skip if this is our own message (deduplication) */
                if (dedup_check_and_record(topic, data)) {
                    ESP_LOGD(TAG, "Duplicate message, skipping");
                    free(topic);
                    free(data);
                    break;
                }

                /* Parse JSON payload if present, otherwise treat as plain text */
                char *content = NULL;
                cJSON *root = cJSON_Parse(data);
                if (root) {
                    /* Try to extract "text" or "message" field from JSON */
                    cJSON *text = cJSON_GetObjectItem(root, "text");
                    cJSON *message = cJSON_GetObjectItem(root, "message");
                    cJSON *payload_field = cJSON_GetObjectItem(root, "payload");

                    const char *extracted = NULL;
                    if (text && cJSON_IsString(text)) {
                        extracted = text->valuestring;
                    } else if (message && cJSON_IsString(message)) {
                        extracted = message->valuestring;
                    } else if (payload_field && cJSON_IsString(payload_field)) {
                        extracted = payload_field->valuestring;
                    }

                    if (extracted && extracted[0]) {
                        content = strdup(extracted);
                    }
                    cJSON_Delete(root);
                }

                /* If not JSON or no recognized field, use raw data */
                if (!content) {
                    content = strdup(data);
                }

                if (content && content[0]) {
                    /* Use topic as chat_id for session routing */
                    mimi_msg_t msg = {0};
                    strncpy(msg.channel, MIMI_CHAN_MQTT, sizeof(msg.channel) - 1);
                    strncpy(msg.chat_id, topic, sizeof(msg.chat_id) - 1);
                    msg.content = content;

                    if (message_bus_push_inbound(&msg) != ESP_OK) {
                        ESP_LOGW(TAG, "Inbound queue full, dropping MQTT message");
                        free(msg.content);
                    } else {
                        ESP_LOGI(TAG, "Message pushed to inbound bus: %s", topic);
                    }
                } else {
                    free(content);
                }

                free(topic);
                free(data);
            }
            break;

        case MQTT_EVENT_ERROR:
            ESP_LOGE(TAG, "MQTT error: %d", event->error_handle->error_type);
            break;

        case MQTT_EVENT_BEFORE_CONNECT:
            ESP_LOGI(TAG, "MQTT connecting to %s...", s_broker_uri);
            break;

        default:
            ESP_LOGD(TAG, "MQTT event: %d", event_id);
            break;
    }
}

/* ── Public API ─────────────────────────────────────────────── */

esp_err_t mqtt_bot_init(void)
{
    /* Start with build-time secrets as defaults */
#ifdef MIMI_SECRET_MQTT_URI
    if (MIMI_SECRET_MQTT_URI[0] != '\0') {
        strncpy(s_broker_uri, MIMI_SECRET_MQTT_URI, sizeof(s_broker_uri) - 1);
    }
#endif
#ifdef MIMI_SECRET_MQTT_CLIENT_ID
    if (MIMI_SECRET_MQTT_CLIENT_ID[0] != '\0') {
        strncpy(s_client_id, MIMI_SECRET_MQTT_CLIENT_ID, sizeof(s_client_id) - 1);
    }
#endif
#ifdef MIMI_SECRET_MQTT_USERNAME
    if (MIMI_SECRET_MQTT_USERNAME[0] != '\0') {
        strncpy(s_username, MIMI_SECRET_MQTT_USERNAME, sizeof(s_username) - 1);
    }
#endif
#ifdef MIMI_SECRET_MQTT_PASSWORD
    if (MIMI_SECRET_MQTT_PASSWORD[0] != '\0') {
        strncpy(s_password, MIMI_SECRET_MQTT_PASSWORD, sizeof(s_password) - 1);
    }
#endif

    /* Load configuration from NVS (overrides build-time) */
    nvs_handle_t nvs;
    if (nvs_open(MIMI_NVS_MQTT, NVS_READONLY, &nvs) == ESP_OK) {
        char tmp[256];
        size_t len;

        len = sizeof(tmp);
        if (nvs_get_str(nvs, MIMI_NVS_KEY_MQTT_URI, tmp, &len) == ESP_OK && tmp[0]) {
            strncpy(s_broker_uri, tmp, sizeof(s_broker_uri) - 1);
        }

        len = sizeof(tmp);
        if (nvs_get_str(nvs, MIMI_NVS_KEY_MQTT_CLIENT_ID, tmp, &len) == ESP_OK && tmp[0]) {
            strncpy(s_client_id, tmp, sizeof(s_client_id) - 1);
        }

        len = sizeof(tmp);
        if (nvs_get_str(nvs, MIMI_NVS_KEY_MQTT_USERNAME, tmp, &len) == ESP_OK && tmp[0]) {
            strncpy(s_username, tmp, sizeof(s_username) - 1);
        }

        len = sizeof(tmp);
        if (nvs_get_str(nvs, MIMI_NVS_KEY_MQTT_PASSWORD, tmp, &len) == ESP_OK && tmp[0]) {
            strncpy(s_password, tmp, sizeof(s_password) - 1);
        }

        len = sizeof(tmp);
        if (nvs_get_str(nvs, MIMI_NVS_KEY_MQTT_SUB_TOPIC, tmp, &len) == ESP_OK && tmp[0]) {
            strncpy(s_subscribe_topic, tmp, sizeof(s_subscribe_topic) - 1);
        }

        nvs_close(nvs);
    }

    /* Check if MQTT is configured */
    if (s_broker_uri[0] != '\0') {
        s_enabled = true;
        ESP_LOGI(TAG, "MQTT configured: %s (client_id=%s)",
                 s_broker_uri,
                 s_client_id[0] ? s_client_id : "(auto)");
    } else {
        ESP_LOGW(TAG, "No MQTT broker configured. Use CLI: set_mqtt_config <uri> [client_id] [username] [password]");
    }

    return ESP_OK;
}

esp_err_t mqtt_bot_start(void)
{
    if (!s_enabled) {
        ESP_LOGW(TAG, "MQTT not configured, skipping start");
        return ESP_OK;
    }

    if (s_mqtt_client != NULL) {
        ESP_LOGW(TAG, "MQTT client already running");
        return ESP_OK;
    }

    /* Generate client ID if not set */
    char client_id[80];
    if (s_client_id[0] == '\0') {
        uint8_t mac[6];
        esp_wifi_get_mac(WIFI_IF_STA, mac);
        snprintf(client_id, sizeof(client_id), "mimiclaw_%02x%02x%02x%02x%02x%02x",
                 mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
    } else {
        strncpy(client_id, s_client_id, sizeof(client_id) - 1);
    }

    /* Configure MQTT client */
    esp_mqtt_client_config_t mqtt_cfg = {
        .broker.address.uri = s_broker_uri,
        .credentials.client_id = client_id,
        .credentials.username = s_username[0] ? s_username : NULL,
        .credentials.authentication.password = s_password[0] ? s_password : NULL,
        .session.keepalive = MIMI_MQTT_KEEPALIVE_S,
        .network.timeout_ms = MIMI_MQTT_TIMEOUT_MS,
        .network.refresh_connection_after_ms = 0,
        .session.disable_clean_session = false,
    };

    s_mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    if (s_mqtt_client == NULL) {
        ESP_LOGE(TAG, "Failed to initialize MQTT client");
        return ESP_FAIL;
    }

    /* Register event handler */
    esp_mqtt_client_register_event(s_mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);

    /* Start the client */
    esp_err_t err = esp_mqtt_client_start(s_mqtt_client);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to start MQTT client: %s", esp_err_to_name(err));
        esp_mqtt_client_destroy(s_mqtt_client);
        s_mqtt_client = NULL;
        return err;
    }

    ESP_LOGI(TAG, "MQTT client started");
    return ESP_OK;
}

esp_err_t mqtt_bot_stop(void)
{
    if (s_mqtt_client == NULL) {
        return ESP_OK;
    }

    esp_err_t err = esp_mqtt_client_stop(s_mqtt_client);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "Error stopping MQTT client: %s", esp_err_to_name(err));
    }

    esp_mqtt_client_destroy(s_mqtt_client);
    s_mqtt_client = NULL;
    s_connected = false;

    ESP_LOGI(TAG, "MQTT client stopped");
    return ESP_OK;
}

esp_err_t mqtt_send_message(const char *topic, const char *text)
{
    if (!s_enabled || s_mqtt_client == NULL) {
        ESP_LOGW(TAG, "Cannot send: MQTT not configured or not started");
        return ESP_ERR_INVALID_STATE;
    }

    if (!s_connected) {
        ESP_LOGW(TAG, "Cannot send: MQTT not connected");
        return ESP_ERR_INVALID_STATE;
    }

    /* Convert request topic to response topic */
    char response_topic[256];
    if (strstr(topic, "/request")) {
        /* Replace /request with /response */
        size_t len = strlen(topic);
        size_t prefix_len = strstr(topic, "/request") - topic;
        snprintf(response_topic, sizeof(response_topic), "%.*s/response", (int)prefix_len, topic);
    } else {
        /* No /request suffix, just use as-is */
        strncpy(response_topic, topic, sizeof(response_topic) - 1);
    }

    /* Split long messages if needed */
    size_t text_len = strlen(text);
    size_t offset = 0;
    int all_ok = 1;

    while (offset < text_len) {
        size_t chunk = text_len - offset;
        if (chunk > MIMI_MQTT_MAX_MSG_LEN) {
            chunk = MIMI_MQTT_MAX_MSG_LEN;
        }

        /* Create chunk with null terminator */
        char *segment = malloc(chunk + 1);
        if (!segment) return ESP_ERR_NO_MEM;
        memcpy(segment, text + offset, chunk);
        segment[chunk] = '\0';

        /* Build JSON payload */
        cJSON *payload = cJSON_CreateObject();
        cJSON_AddStringToObject(payload, "text", segment);
        cJSON_AddStringToObject(payload, "source", "mimiclaw");
        char *json_str = cJSON_PrintUnformatted(payload);
        cJSON_Delete(payload);
        free(segment);

        if (!json_str) {
            offset += chunk;
            all_ok = 0;
            continue;
        }

        /* Publish message */
        int msg_id = esp_mqtt_client_publish(s_mqtt_client, response_topic, json_str, 0, 1, 0);
        free(json_str);

        if (msg_id < 0) {
            ESP_LOGE(TAG, "Failed to publish to %s", response_topic);
            all_ok = 0;
        } else {
            ESP_LOGI(TAG, "Published to %s (%d bytes, msg_id=%d)", response_topic, (int)chunk, msg_id);
        }

        offset += chunk;
    }

    return all_ok ? ESP_OK : ESP_FAIL;
}

esp_err_t mqtt_set_config(const char *broker_uri, const char *client_id,
                          const char *username, const char *password)
{
    nvs_handle_t nvs;
    esp_err_t err = nvs_open(MIMI_NVS_MQTT, NVS_READWRITE, &nvs);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to open NVS: %s", esp_err_to_name(err));
        return err;
    }

    /* Save broker URI (required) */
    if (broker_uri && broker_uri[0]) {
        nvs_set_str(nvs, MIMI_NVS_KEY_MQTT_URI, broker_uri);
        strncpy(s_broker_uri, broker_uri, sizeof(s_broker_uri) - 1);
    }

    /* Save client ID (optional) */
    if (client_id && client_id[0]) {
        nvs_set_str(nvs, MIMI_NVS_KEY_MQTT_CLIENT_ID, client_id);
        strncpy(s_client_id, client_id, sizeof(s_client_id) - 1);
    }

    /* Save username (optional) */
    if (username && username[0]) {
        nvs_set_str(nvs, MIMI_NVS_KEY_MQTT_USERNAME, username);
        strncpy(s_username, username, sizeof(s_username) - 1);
    } else {
        nvs_erase_key(nvs, MIMI_NVS_KEY_MQTT_USERNAME);
        s_username[0] = '\0';
    }

    /* Save password (optional) */
    if (password && password[0]) {
        nvs_set_str(nvs, MIMI_NVS_KEY_MQTT_PASSWORD, password);
        strncpy(s_password, password, sizeof(s_password) - 1);
    } else {
        nvs_erase_key(nvs, MIMI_NVS_KEY_MQTT_PASSWORD);
        s_password[0] = '\0';
    }

    nvs_commit(nvs);
    nvs_close(nvs);

    s_enabled = (s_broker_uri[0] != '\0');
    ESP_LOGI(TAG, "MQTT config saved: %s", s_broker_uri);

    return ESP_OK;
}

esp_err_t mqtt_set_subscribe_topic(const char *topic_pattern)
{
    if (!topic_pattern || !topic_pattern[0]) {
        return ESP_ERR_INVALID_ARG;
    }

    nvs_handle_t nvs;
    esp_err_t err = nvs_open(MIMI_NVS_MQTT, NVS_READWRITE, &nvs);
    if (err != ESP_OK) {
        return err;
    }

    nvs_set_str(nvs, MIMI_NVS_KEY_MQTT_SUB_TOPIC, topic_pattern);
    nvs_commit(nvs);
    nvs_close(nvs);

    strncpy(s_subscribe_topic, topic_pattern, sizeof(s_subscribe_topic) - 1);
    ESP_LOGI(TAG, "Subscribe topic set: %s", s_subscribe_topic);

    /* Re-subscribe if already connected */
    if (s_connected && s_mqtt_client) {
        esp_mqtt_client_subscribe(s_mqtt_client, s_subscribe_topic, 1);
    }

    return ESP_OK;
}

bool mqtt_is_connected(void)
{
    return s_connected;
}
