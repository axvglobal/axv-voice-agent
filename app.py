from flask import Flask, Response
from twilio.twiml.voice_response import VoiceResponse

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return "AXV Voice Agent is running."

@app.route("/twilio/voice", methods=["POST", "GET"])
def twilio_voice():
    response = VoiceResponse()

    response.say(
        "Hello, thank you for calling AXV Global. "
        "This is Alex, your virtual assistant. "
        "We are currently testing our voice system.",
        voice="alice",
        language="en-US"
    )

    response.pause(length=1)

    response.say(
        "Please call again later. Thank you.",
        voice="alice",
        language="en-US"
    )

    response.hangup()

    return Response(str(response), mimetype="text/xml")