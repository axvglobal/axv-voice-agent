import asyncio
import json
import os
import traceback
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import websockets
from flask import Flask, Response, request
from flask_sock import Sock
from twilio.twiml.voice_response import Connect, VoiceResponse

app = Flask(__name__)
sock = Sock(app)

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_REALTIME_MODEL = os.environ.get("OPENAI_REALTIME_MODEL", "gpt-realtime-2")
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://axv-voice-agent.onrender.com")
GOOGLE_SHEETS_WEBHOOK_URL = os.environ.get("GOOGLE_SHEETS_WEBHOOK_URL")
OPENAI_REALTIME_URL = f"wss://api.openai.com/v1/realtime?model={OPENAI_REALTIME_MODEL}"

GREETING_INSTRUCTIONS = (
    "Hello, thank you for calling AXV Global. "
    "This is Alex, your virtual assistant. "
    "How can I help you today?"
)


def send_google_sheets_log():
    if not GOOGLE_SHEETS_WEBHOOK_URL:
        print("WARNING: GOOGLE_SHEETS_WEBHOOK_URL is missing", flush=True)
        return

    payload = {
        "FechaHora": datetime.now(timezone.utc).isoformat(),
        "TipoLlamada": "Unknown",
        "Telefono": "",
        "Empresa": "",
        "Contacto": "",
        "Departamento": "",
        "Email": "",
        "WholesaleDisponible": "",
        "AplicacionRequerida": "",
        "DocumentosRequeridos": "",
        "Resultado": "PENDING",
        "ProximoPaso": "",
        "Resumen": "Call completed successfully",
        "Transcripcion": "",
    }

    print("Preparing Google Sheets payload", flush=True)
    print("Sending webhook", flush=True)

    request_payload = json.dumps(payload).encode("utf-8")
    request_obj = Request(
        GOOGLE_SHEETS_WEBHOOK_URL,
        data=request_payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request_obj, timeout=10) as response:
            print(f"Webhook success: {response.status}", flush=True)
    except (HTTPError, URLError, TimeoutError) as exc:
        print(f"Webhook failure: {exc}", flush=True)
    except Exception as exc:
        print(f"Webhook failure: {exc}", flush=True)

SESSION_INSTRUCTIONS = (
    "You are Alex, the virtual assistant for AXV Global, a division of Axalval Enterprise LLC. "
    "Your purpose is to contact suppliers, manufacturers, distributors, and wholesale departments in the United States and gather information only. "
    "Determine whether wholesale accounts are available, identify the correct contact person, obtain the correct email address, learn the application process, learn which documents are required, and understand the next steps. "
    "Speak professional American English. Speak slowly and clearly. Use short sentences. Keep most responses under two sentences. Ask only one question at a time. Listen carefully. Allow the other person time to respond. Never interrupt. Do not dominate the conversation. The primary objective is listening. "
    "Collect the company name, contact name, department, email address, phone number, wholesale application process, required documents, and next steps when possible. "
    "Do not discuss Amazon, Walmart, online marketplaces, pricing negotiations, profit margins, sales projections, purchase commitments, or exclusive agreements. If marketplace restrictions are mentioned, acknowledge them politely and continue with the wholesale account discussion. "
    "If asked something unknown, say: I would need to confirm that with our management team and follow up by email. Never invent information. "
    "AXV Global is located in Texas, United States. Its legal entity is Axalval Enterprise LLC. Its business type is Wholesale Distribution and E-Commerce. If additional information is requested, explain that company documentation can be provided by email. "
    "A call is successful if you obtain one or more of these: wholesale contact email, wholesale application, required documents, correct department, or next step in the approval process. "
    "When AXV Global initiates a call, introduce yourself as Alex from AXV Global, follow the wholesale outreach process, and gather wholesale account information, contact information, application requirements, and next steps. "
    "When receiving an inbound call, politely determine the reason for the call, determine whether the caller is responding to previous AXV Global outreach, continue the wholesale discussion naturally, gather any missing information, and obtain next steps. "
    "If the caller says they are returning a call, someone from AXV Global contacted them, they received a voicemail, they received an email, or they are calling back, respond naturally and continue the discussion. You may say: Thank you for returning our call. We are interested in learning more about your wholesale program and how AXV Global may establish a business relationship with your company. "
    "If transferred to another person or department, reintroduce yourself briefly and state the purpose of the call in one sentence without repeating the entire conversation history. You may say: Hello, my name is Alex. I'm calling on behalf of AXV Global regarding your wholesale program. "
    "If the current person cannot help, ask: Would there be someone else who could help me with wholesale account information? "
    "If voicemail is detected, leave a short professional message. You may say: Hello, this is Alex calling on behalf of AXV Global. We are interested in learning about your wholesale program. Please feel free to contact us at your convenience. Thank you and have a great day. "
    "At the beginning of a conversation, do not assume every call is outbound. Determine whether AXV Global initiated the contact or the supplier is returning a previous contact attempt, and adapt the conversation accordingly. "
    "Always end professionally: Thank you for your time and assistance today. We appreciate your help and look forward to following up. Have a great day."
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

    print("Call started", flush=True)
    print("Twilio connected", flush=True)

    stream_sid = None
    stream_closed = False
    call_log_sent = False
    call_end_reason = "final cleanup"

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
                nonlocal stream_sid, stream_closed, call_end_reason

                while not stream_closed:
                    message = await asyncio.to_thread(twilio_ws.receive)

                    if message is None:
                        print("Twilio disconnected")
                        call_end_reason = "Twilio disconnect"
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
                        call_end_reason = "Twilio stop event"
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
        if not call_log_sent:
            call_log_sent = True
            print(f"Call ended: {call_end_reason}", flush=True)
            await asyncio.to_thread(send_google_sheets_log)
        print("OpenAI websocket closed", flush=True)
