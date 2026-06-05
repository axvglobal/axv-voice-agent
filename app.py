import os
import json
import base64
import asyncio
import websockets

from flask import Flask, Response
from flask_sock import Sock
from twilio.twiml.voice_response import VoiceResponse, Connect

app = Flask(__name__)
sock = Sock(app)

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

OPENAI_REALTIME_URL = (
    "wss://api.openai.com/v1/realtime"
    "?model=gpt-4o-realtime-preview"
)


@app.route("/", methods=["GET"])
def home():
    return "AXV Voice Agent is running."


@app.route("/twilio/voice", methods=["POST", "GET"])
def twilio_voice():
    response = VoiceResponse()
    connect = Connect()

    connect.stream(
        url="wss://axv-voice-agent.onrender.com/media-stream"
    )

    response.append(connect)

    return Response(str(response), mimetype="text/xml")


@sock.route("/media-stream")
def media_stream(ws):
    print("Twilio connected to /media-stream")

    asyncio.run(handle_media_stream(ws))


async def handle_media_stream(twilio_ws):
    stream_sid = None

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "OpenAI-Beta": "realtime=v1",
    }

    async with websockets.connect(
        OPENAI_REALTIME_URL,
        extra_headers=headers,
    ) as openai_ws:

        await openai_ws.send(json.dumps({
            "type": "session.update",
            "session": {
                "modalities": ["text", "audio"],
                "voice": "alloy",
                "instructions": (
                    "You are Alex, a professional virtual assistant for AXV Global, "
                    "a U.S.-based e-commerce and distribution company. "
                    "Speak clearly in professional American English. "
                    "Your first goal is to greet the caller and explain that this is a test call. "
                    "Keep responses short and natural."
                ),
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "turn_detection": {
                    "type": "server_vad"
                }
            }
        }))

        await openai_ws.send(json.dumps({
            "type": "response.create",
            "response": {
                "modalities": ["audio", "text"],
                "instructions": (
                    "Greet the caller by saying: "
                    "Hello, thank you for calling AXV Global. "
                    "This is Alex, your virtual assistant. "
                    "How can I help you today?"
                )
            }
        }))

        async def receive_from_twilio():
            nonlocal stream_sid

            while True:
                message = twilio_ws.receive()

                if message is None:
                    print("Twilio disconnected")
                    break

                data = json.loads(message)

                if data["event"] == "start":
                    stream_sid = data["start"]["streamSid"]
                    print(f"Stream started: {stream_sid}")

                elif data["event"] == "media":
                    await openai_ws.send(json.dumps({
                        "type": "input_audio_buffer.append",
                        "audio": data["media"]["payload"]
                    }))

                elif data["event"] == "stop":
                    print("Stream stopped")
                    break

        async def send_to_twilio():
            nonlocal stream_sid

            async for openai_message in openai_ws:
                response = json.loads(openai_message)

                if response.get("type") == "response.audio.delta":
                    audio_payload = response.get("delta")

                    if stream_sid and audio_payload:
                        twilio_ws.send(json.dumps({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {
                                "payload": audio_payload
                            }
                        }))

                elif response.get("type") == "error":
                    print("OpenAI error:", response)

        await asyncio.gather(
            receive_from_twilio(),
            send_to_twilio()
        )