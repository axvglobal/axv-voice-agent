from flask import Flask, Response
from flask_sock import Sock
from twilio.twiml.voice_response import VoiceResponse, Connect

app = Flask(__name__)
sock = Sock(app)

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

    print("Client connected to media stream")

    while True:

        data = ws.receive()

        if data is None:
            break

        print(data)

    print("Client disconnected")