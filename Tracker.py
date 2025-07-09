from flask import Flask, request

app = Flask(__name__)


# Endpoint to track user-agent
@app.route('/track', methods=['GET'])
def track_user():
    # Get user-agent from the request headers
    user_agent = request.headers.get('User-Agent')
    ip_address = request.remote_addr  # Optional: Log the requester's IP address

    # Log user-agent details
    with open('user_agent_logs.txt', 'a') as log_file:
        log_file.write(f"User-Agent: {user_agent}, IP: {ip_address}\n")

    # Respond to the user
    return "Thank you! Your details have been logged.", 200


if __name__ == '__main__':
    # Run the web server on port 5000
    app.run(host='0.0.0.0', port=5000)
