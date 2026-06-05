import asyncio
import json
import os
import traceback

import websockets
from flask import Flask, Response, request
from flask_sock import Sock
from twilio.twiml.voice_response import Connect, VoiceResponse

app = Flask(__name__)
sock = Sock(app)

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_REALTIME_MODEL = os.environ.get("OPENAI_REALTIME_MODEL", "gpt-realtime-2")
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://axv-voice-agent.onrender.com")
OPENAI_REALTIME_URL = f"wss://api.openai.com/v1/realtime?model={OPENAI_REALTIME_MODEL}"

GREETING_INSTRUCTIONS = (
    "Hello, thank you for calling AXV Global. "
    "This is Alex, your virtual assistant. "
    "How can I help you today?"
)

SESSION_INSTRUCTIONS = (
    "You are Alex, a professional virtual assistant for AXV Global, "
    "a U.S.-based e-commerce and distribution company. "
    "Speak clearly in professional American English. "
    "Keep responses short, natural, and helpful. "
    "If the caller is just starting, begin with the greeting provided by the system."
)


@app.route("/", methods=["GET"])
def home():
    return "AXV Voice Agent is running."


@app.route("/twilio/voice", methods=["POST", "GET"])
def twilio_voice():
    response = VoiceResponse()
    connect = Connect()
    connect.stream(
        url="wss://axv-voice-agent.onrender.com/media-stream",
        status_callback="https://axv-voice-agent.onrender.com/stream-status",
        status_callback_method="POST"
    )
    response.append(connect)
    return Response(str(response), mimetype="text/xml")


@app.route("/stream-status", methods=["POST"])
def stream_status():
    print("STREAM STATUS:", dict(request.form), flush=True)
    return "OK", 200


@sock.route("/media-stream")
def media_stream(ws):
    print("Twilio connected to /media-stream")
    asyncio.run(handle_media_stream(ws))


async def handle_media_stream(twilio_ws):
    if not OPENAI_API_KEY:
        print("Missing OPENAI_API_KEY")
        return

    print("Twilio connected", flush=True)

    stream_sid = None
    stream_closed = False

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
    }

    try:
        async with websockets.connect(
            OPENAI_REALTIME_URL,
            extra_headers=headers,
        ) as openai_ws:
            print("OpenAI connected", flush=True)

            print("Sending session.update", flush=True)
            await openai_ws.send(json.dumps({
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "model": OPENAI_REALTIME_MODEL,
                    "instructions": SESSION_INSTRUCTIONS,
                    "audio": {
                        "input": {
                            "format": {
                                "type": "audio/pcmu"
                            },
                            "turn_detection": {
                                "type": "server_vad",
                                "create_response": True,
                                "interrupt_response": True
                            }
                        },
                        "output": {
                            "format": {
                                "type": "audio/pcmu"
                            },
                            "voice": "marin"
                        }
                    },
                    "output_modalities": ["audio"]
                }
            }))
            print("session.update sent", flush=True)

            print("Sending response.create", flush=True)
            await openai_ws.send(json.dumps({
                "type": "response.create",
                "response": {
                    "instructions": "Say exactly: Hello, thank you for calling AXV Global. This is Alex, your virtual assistant. How can I help you today?"
                }
            }))
            print("response.create sent", flush=True)

            async def receive_from_twilio():
                nonlocal stream_sid, stream_closed

                while not stream_closed:
                    message = await asyncio.to_thread(twilio_ws.receive)

                    if message is None:
                        print("Twilio disconnected")
                        stream_closed = True
                        await openai_ws.close()
                        break

                    data = json.loads(message)
                    event_type = data.get("event")

                    if event_type == "start":
                        stream_sid = data["start"]["streamSid"]
                        print(f"Stream started: {stream_sid}")
                        print("STREAM SID:", stream_sid, flush=True)

                    elif event_type == "media":
                        await openai_ws.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": data["media"]["payload"]
                        }))

                    elif event_type == "stop":
                        print("Stream stopped")
                        stream_closed = True
                        await openai_ws.close()
                        break

            async def send_to_twilio():
                nonlocal stream_sid, stream_closed

                try:
                    async for message in openai_ws:
                        response = json.loads(message)
                        print("OPENAI EVENT:", response, flush=True)
                        event_type = response.get("type")

                        if event_type == "response.output_audio.delta":
                            audio_payload = response.get("delta")
                            if stream_sid and audio_payload:
                                print("FORWARDING AUDIO TO TWILIO", flush=True)
                                print("AUDIO CHUNK SIZE:", len(audio_payload), flush=True)
                                await asyncio.to_thread(
                                    twilio_ws.send,
                                    json.dumps({
                                        "event": "media",
                                        "streamSid": stream_sid,
                                        "media": {
                                            "payload": audio_payload
                                        }
                                    })
                                )

                        elif event_type == "error":
                            print("OpenAI error:", response)
                except Exception as e:
                    print("OPENAI EXCEPTION:", str(e), flush=True)
                    traceback.print_exc()
                finally:
                    print("OpenAI websocket closed", flush=True)
                    stream_closed = True

            await asyncio.gather(
                receive_from_twilio(),
                send_to_twilio()
            )
    except Exception as e:
        print("OPENAI EXCEPTION:", str(e), flush=True)
        traceback.print_exc()
    finally:
        print("OpenAI websocket closed", flush=True)
