from hop import Stream
from hop.auth import load_auth
# Send and receive GCN notices


# Send the message through HOPSKOTCH
def submit_message_to_hop(message: dict):
    """Send a message through the HOPSKOTCH kafka broker

    message: dict
        The message to send. Message must be json serializable
    """
    stream = Stream(auth=load_auth())
    with stream.open("kafka://kafka.scimma.org/tarxiv", "w") as s:
        s.write(message)
