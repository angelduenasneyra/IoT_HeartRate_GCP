// Function: Detection of heart rate and blood oxygen concentration
// Units used: M5StickCPlus, Heart(MAX30102)
// please install MAX3010x lib for MAX30102 by library manager first
// addr: https://github.com/sparkfun/SparkFun_MAX3010x_Sensor_Library
#include <M5StickCPlus.h>
#include <Wire.h>
#include "MAX30105.h"
#include "heartRate.h"
#include "spo2_algorithm.h"
#include <WiFi.h>
#include <PubSubClient.h>

const char* ntpServer = "pool.ntp.org";
const long  gmtOffset_sec = -21600; // UTC-6 hours in seconds for Austin, Texas
const int   daylightOffset_sec = 3600; // DST is 1 hour (3600 seconds)


const char* ssid = "BonnieWifi";
const char* password = "6105Bonnie";
const char* mqtt_broker = "35.184.8.199";
const char* mqtt_username = "msquser";
const char* mqtt_password = "msqpass"; 
const int mqtt_port = 1883;
const char* mqtt_topic = "topictest";

WiFiClient espClient;
PubSubClient client(espClient);

// Function to connect to WiFi
void setup_wifi() {
    delay(10);
    Serial.println("Connecting to WiFi...");
    WiFi.begin(ssid, password);

    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    Serial.println("WiFi connected");
}


void reconnect() {
    while (!client.connected()) {
        Serial.print("Attempting MQTT connection...");
        if (client.connect("M5StickCPlus", mqtt_username, mqtt_password)) {
            Serial.println("connected");
        } else {
            Serial.print("failed, rc=");
            Serial.print(client.state());
            Serial.println(" try again in 5 seconds");
            delay(5000);
        }
    }
}




void publishData(int spo2, int bpm) {
    char msg[100];
    String timeStr = currentTimeString();
    sprintf(msg, "id0001, %s, spo2: %d, BPM: %d", timeStr.c_str(), spo2, bpm);
    Serial.print("Publishing: ");
    Serial.println(msg);
    if (client.publish(mqtt_topic, msg)) {
        Serial.println("Publish successful");
    } else {
        Serial.println("Publish failed");
    }
}


// Helper function to get current time as string
char* currentTimeString() {
    struct tm timeinfo;
    if (!getLocalTime(&timeinfo)) {
        Serial.println("Failed to obtain time");
        return "0000-00-00 00:00:00";
    }
    static char timeStringBuff[30];
    strftime(timeStringBuff, sizeof(timeStringBuff), "%Y-%m-%d %H:%M:%S", &timeinfo);
    return timeStringBuff;
}


TFT_eSprite Disbuff = TFT_eSprite(&M5.Lcd);
MAX30105 Sensor;

#define MAX_BRIGHTNESS 255
#define bufferLength   100
const byte Button_A = 37;
const byte pulseLED = 26;

uint32_t irBuffer[bufferLength];
uint32_t redBuffer[bufferLength];

int8_t V_Button, flag_Reset;
int32_t spo2, heartRate, old_spo2;
int8_t validSPO2, validHeartRate;
const byte RATE_SIZE = 5;
uint16_t rate_begin  = 0;
uint16_t rates[RATE_SIZE];
byte rateSpot = 0;
float beatsPerMinute;
int beatAvg;
byte num_fail;

uint16_t line[2][320] = {0};

uint32_t red_pos = 0, ir_pos = 0;
uint16_t ir_max = 0, red_max = 0, ir_min = 0, red_min = 0, ir_last = 0,
         red_last    = 0;
uint16_t ir_last_raw = 0, red_last_raw = 0;
uint16_t ir_disdata, red_disdata;
uint16_t Alpha = 0.3 * 256;
uint32_t t1, t2, last_beat, Program_freq;

void callBack(void) {
    V_Button = digitalRead(Button_A);
    if (V_Button == 0) flag_Reset = 1;
    delay(10);
}

void setup() {
    // init
    M5.begin();                 // initialize M5StickCPlus
    Serial.begin(115200);       // initialize serial communication
    pinMode(25, INPUT_PULLUP);  // set pin mode
    pinMode(pulseLED, OUTPUT);
    pinMode(Button_A, INPUT);
    Wire.begin(0, 26);  // initialize I2C

    // set Lcd
    M5.Lcd.setRotation(3);
    M5.Lcd.setSwapBytes(false);
    Disbuff.createSprite(240, 135);
    Disbuff.setSwapBytes(true);
    Disbuff.createSprite(240, 135);

    // initialize Sensor
    if (!Sensor.begin(Wire, I2C_SPEED_FAST)) {
        // init fail
        M5.Lcd.print("Init Failed");
        Serial.println(F("MAX30102 was not found. Please check wiring/power."));
        while (1)
            ;
    }
    Serial.println(
        F("Place your index finger on the Sensor with steady pressure"));

    attachInterrupt(Button_A, callBack, FALLING);
    // set Max30102
    Sensor.setup();
    // Sensor.clearFIFO();
    setup_wifi();
    client.setServer(mqtt_broker, mqtt_port);
    reconnect();
    configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);
}

void loop() {
    // put your main code here, to run repeatedly:
    uint16_t ir, red;

    Serial.println("Top of loop");

    if (!client.connected()) {
        Serial.println("Reconnecting to MQTT...");
        reconnect();
    }
    client.loop();
    Serial.println("MQTT client loop finished");

    // Check flag_Reset
    Serial.print("flag_Reset: ");
    Serial.println(flag_Reset);

    if (flag_Reset) {
        Serial.println("Clearing Sensor FIFO...");
        Sensor.clearFIFO();
        delay(5);
        flag_Reset = 0;
    }

    Serial.println("Entering main while loop");
    while (flag_Reset == 0) {
        while (Sensor.available() == false) {
            delay(10);
            Sensor.check();
        }

        while (1) {
            red = Sensor.getRed();
            ir  = Sensor.getIR();

            if ((ir > 1000) && (red > 1000)) {
                num_fail                         = 0;
                t1                               = millis();
                redBuffer[(red_pos + 100) % 100] = red;
                irBuffer[(ir_pos + 100) % 100]   = ir;

                // calculate the heart rate
                t2 = millis();
                Program_freq++;
                if (checkForBeat(ir) == true) {
                    long delta =
                        millis() - last_beat - (t2 - t1) * (Program_freq - 1);
                    last_beat = millis();

                    Program_freq   = 0;
                    beatsPerMinute = 60 / (delta / 1000.0);
                    if ((beatsPerMinute > 30) && (beatsPerMinute < 120)) {
                        rate_begin++;
                        if ((abs(beatsPerMinute - beatAvg) > 15) &&
                            ((beatsPerMinute < 55) || (beatsPerMinute > 95)))
                            beatsPerMinute =
                                beatAvg * 0.9 + beatsPerMinute * 0.1;
                        if ((abs(beatsPerMinute - beatAvg) > 10) &&
                            (beatAvg > 60) &&
                            ((beatsPerMinute < 65) || (beatsPerMinute > 90)))
                            beatsPerMinute =
                                beatsPerMinute * 0.4 + beatAvg * 0.6;

                        rates[rateSpot++] = (byte)
                            beatsPerMinute;  // Store this reading in the array
                        rateSpot %= RATE_SIZE;  // Wrap variable

                        // Take average of readings
                        beatAvg = 0;
                        if ((beatsPerMinute == 0) && (heartRate > 60) &&
                            (heartRate < 90))
                            beatsPerMinute = heartRate;
                        if (rate_begin > RATE_SIZE) {
                            for (byte x = 0; x < RATE_SIZE; x++)
                                beatAvg += rates[x];
                            beatAvg /= RATE_SIZE;
                        } else {
                            for (byte x = 0; x < rate_begin; x++)
                                beatAvg += rates[x];
                            beatAvg /= rate_begin;
                        }
                    }
                }
            }

            else
                num_fail++;

            line[0][(red_pos + 240) % 320] =
                (red_last_raw * (256 - Alpha) + red * Alpha) / 256;
            line[1][(ir_pos + 240) % 320] =
                (ir_last_raw * (256 - Alpha) + ir * Alpha) / 256;

            red_last_raw = line[0][(red_pos + 240) % 320];
            ir_last_raw  = line[1][(ir_pos + 240) % 320];
            red_pos++;
            ir_pos++;

            if ((Sensor.check() == false) || flag_Reset) break;
        }

        Sensor.clearFIFO();
        for (int i = 0; i < 240; i++) {
            if (i == 0) {
                red_max = red_min = line[0][(red_pos + i) % 320];
                ir_max = ir_min = line[1][(ir_pos + i) % 320];
            } else {
                red_max = (line[0][(red_pos + i) % 320] > red_max)
                              ? line[0][(red_pos + i) % 320]
                              : red_max;
                red_min = (line[0][(red_pos + i) % 320] < red_min)
                              ? line[0][(red_pos + i) % 320]
                              : red_min;

                ir_max = (line[1][(ir_pos + i) % 320] > ir_max)
                             ? line[1][(ir_pos + i) % 320]
                             : ir_max;
                ir_min = (line[1][(ir_pos + i) % 320] < ir_min)
                             ? line[1][(ir_pos + i) % 320]
                             : ir_min;
            }
            if (flag_Reset) break;
        }

        Disbuff.fillRect(0, 0, 240, 135, BLACK);

        for (int i = 0; i < 240; i++) {
            red_disdata =
                map(line[0][(red_pos + i) % 320], red_max, red_min, 0, 135);
            ir_disdata =
                map(line[1][(ir_pos + i) % 320], ir_max, ir_min, 0, 135);
            {
                Disbuff.drawLine(i, red_last, i + 1, red_disdata, RED);
                Disbuff.drawLine(i, ir_last, i + 1, ir_disdata, BLUE);
            }
            ir_last  = ir_disdata;
            red_last = red_disdata;
            if (flag_Reset) break;
        }
        old_spo2 = spo2;
        if (red_pos > 100)
            maxim_heart_rate_and_oxygen_saturation(irBuffer, bufferLength,
                                                   redBuffer, &spo2, &validSPO2,
                                                   &heartRate, &validHeartRate);

        if (!validSPO2) spo2 = old_spo2;

        Disbuff.setTextSize(1);
        Disbuff.setTextColor(RED);
        Disbuff.setCursor(5, 5);
        Disbuff.printf("red:%d,%d,%d", red_max, red_min, red_max - red_min);
        Disbuff.setTextColor(BLUE);
        Disbuff.setCursor(5, 15);
        Disbuff.printf("ir:%d,%d,%d", ir_max, ir_min, ir_max - ir_min);
        Disbuff.setCursor(5, 25);
        if (num_fail < 10) {
            Disbuff.setTextColor(GREEN);
            Disbuff.printf("spo2:%d,", spo2);
            Disbuff.setCursor(60, 25);
            Disbuff.printf(" BPM:%d", beatAvg);
        } else {
            Disbuff.setTextColor(RED);
            Disbuff.printf("No Finger!!");
        }
        Disbuff.pushSprite(0, 0);
        static unsigned long lastPublish = 0;
      if (millis() - lastPublish > 5000) {
          lastPublish = millis();
          publishData(spo2, beatAvg);
      }
    }
}
