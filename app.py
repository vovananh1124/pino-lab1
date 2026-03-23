import os
import logging
import pyodbc

from flask import Flask, jsonify, Response
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# =========================
# Environment variables
# =========================
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
STORAGE_ACCOUNT_URL = os.getenv("STORAGE_ACCOUNT_URL")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "user-images")

if not DB_CONNECTION_STRING:
    raise ValueError("Missing DB_CONNECTION_STRING in App Settings")

if not STORAGE_ACCOUNT_URL:
    raise ValueError("Missing STORAGE_ACCOUNT_URL in App Settings")

# Managed Identity on Azure App Service
credential = DefaultAzureCredential()


# =========================
# Helpers
# =========================
def get_db_connection():
    return pyodbc.connect(DB_CONNECTION_STRING)


def get_blob_service_client():
    return BlobServiceClient(
        account_url=STORAGE_ACCOUNT_URL,
        credential=credential
    )


# =========================
# Routes
# =========================
@app.route("/")
def home():
    return jsonify({
        "message": "Azure App Service is running",
        "status": "ok"
    })


@app.route("/health")
def health():
    result = {
        "app": "ok",
        "database": "unknown",
        "blob_storage": "unknown"
    }

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        result["database"] = "ok"
    except Exception as e:
        logging.exception("Database health check failed")
        result["database"] = f"error: {str(e)}"

    try:
        blob_service_client = get_blob_service_client()
        container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
        container_client.get_container_properties()
        result["blob_storage"] = "ok"
    except Exception as e:
        logging.exception("Blob storage health check failed")
        result["blob_storage"] = f"error: {str(e)}"

    return jsonify(result)


@app.route("/user/<int:user_id>")
def get_user(user_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
        SELECT id, name, age, phone_number, address, image_blob_name
        FROM Users
        WHERE id = ?
        """
        cursor.execute(query, user_id)
        row = cursor.fetchone()

        cursor.close()
        conn.close()

        if not row:
            return jsonify({"error": "User not found"}), 404

        return jsonify({
            "id": row[0],
            "name": row[1],
            "age": row[2],
            "phone_number": row[3],
            "address": row[4],
            "image_blob_name": row[5],
            "image_api": f"/user/{row[0]}/image"
        })

    except Exception as e:
        logging.exception("Failed to get user")
        return jsonify({"error": str(e)}), 500


@app.route("/user/<int:user_id>/image")
def get_user_image(user_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = "SELECT image_blob_name FROM Users WHERE id = ?"
        cursor.execute(query, user_id)
        row = cursor.fetchone()

        cursor.close()
        conn.close()

        if not row or not row[0]:
            return jsonify({"error": "Image not found"}), 404

        blob_name = row[0]

        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=BLOB_CONTAINER_NAME,
            blob=blob_name
        )

        download_stream = blob_client.download_blob()
        blob_data = download_stream.readall()

        props = blob_client.get_blob_properties()
        content_type = props.content_settings.content_type or "application/octet-stream"

        return Response(blob_data, mimetype=content_type)

    except Exception as e:
        logging.exception("Failed to get image")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
