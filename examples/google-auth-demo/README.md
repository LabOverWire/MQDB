# Google OAuth Sign-Up/Sign-In Demo

A web-based example demonstrating Google OAuth sign-in, MQTT over WebSocket connection, and MQDB CRUD operations for user profile management.

## Prerequisites

- MQDB built (`cargo build --release`)
- Python 3 (for the HTTP server)
- A Google Cloud OAuth 2.0 Client ID

## Google Cloud Setup

1. Go to [Google Cloud Console - Credentials](https://console.cloud.google.com/apis/credentials)
2. Create an **OAuth 2.0 Client ID** (type: Web application)
3. Add `http://localhost:8000` as an **Authorized JavaScript origin**
4. Copy the **Client ID** (ends with `.apps.googleusercontent.com`)

## Running

```bash
# Option 1: Set client ID via environment
export GOOGLE_CLIENT_ID="your-client-id.apps.googleusercontent.com"
./examples/google-auth-demo/run.sh

# Option 2: Enter interactively (saved for next run)
./examples/google-auth-demo/run.sh
```

Open http://localhost:8000 in your browser.

## What It Does

1. **Google Sign-In** - Authenticates with Google and obtains a JWT ID token
2. **MQTT Connection** - Connects to MQDB over WebSocket using the JWT for authentication (MQTT v5 enhanced auth)
3. **Auto Sign-Up/Sign-In** - On connect, queries `$DB/users` for your Google `sub` claim. Creates a profile if not found.
4. **Profile Management** - Edit name/bio, delete account via MQDB CRUD operations
5. **Real-Time Events** - Subscribe to `$DB/users/events/#` for live user change notifications

## Architecture

```
Browser (index.html)
  |
  |-- Google Identity Services (OAuth JWT)
  |-- mqtt.js (MQTT v5 over WebSocket)
        |
        v
MQDB Agent (port 1883 TCP, port 8080 WebSocket)
  |-- Federated JWT validation (Google's JWKS)
  |-- Embedded database ($DB/users entity)
  |-- Change event publishing ($DB/users/events/#)
```

## Ports

| Port | Service |
|------|---------|
| 1883 | MQTT (TCP) |
| 8080 | MQTT (WebSocket) |
| 8000 | HTTP (static files) |
