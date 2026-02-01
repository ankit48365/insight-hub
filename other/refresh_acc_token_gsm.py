"""Script to refresh Strava access token using the refresh token."""
import requests
import dlt
import os


def refresh_access_token():
    """Refreshes the Strava access token using the refresh token."""

    # Try Cloud Run env vars first
    client_id = os.getenv("SOURCES__STRAVA__CLIENT_ID")
    client_secret = os.getenv("SOURCES__STRAVA__CLIENT_SECRET")
    access_token = os.getenv("SOURCES__STRAVA__ACCESS_TOKEN")
    refresh_token = os.getenv("SOURCES__STRAVA__REFRESH_TOKEN")

    # Fallback to local secrets if any are missing
    if not all([client_id, client_secret, access_token, refresh_token]):
        sec = dlt.secrets.get("sources.strava")
        client_id = client_id or sec.get("client_id")
        client_secret = client_secret or sec.get("client_secret")
        access_token = access_token or sec.get("access_token")
        refresh_token = refresh_token or sec.get("refresh_token")

    print("🔄 Refreshing Strava access token...")

    # Make request to refresh token
    response = requests.post("https://www.strava.com/oauth/token", data={
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    })
    
    if response.status_code == 200:
        token_data = response.json()
        new_access_token = token_data["access_token"]
        new_refresh_token = token_data["refresh_token"]
        
        print(f"✅ New access token: {new_access_token[:8]}...")
        print(f"✅ New refresh token: {new_refresh_token[:8]}...")
        
        # Update the secrets file
        update_secrets_file(new_access_token, new_refresh_token, client_id, client_secret)
        
        return True
    else:
        print(f"❌ Failed to refresh token. Status: {response.status_code}")
        print(f"Response: {response.text}")
        return False


###################################################################
## add for cloud secret manager
# to fake cloud environment, run before pythonn code run >> export K_SERVICE="fake" or export K_SERVICE="strava-job"
# to unset run from terminal >> unset K_SERVICE

def is_running_in_cloud():
    # Cloud Run Service
    if os.getenv("K_SERVICE"):
        return True

    # Cloud Run Job
    if os.getenv("CLOUD_RUN_JOB"):
        return True

    return False



from google.cloud import secretmanager

def update_cloud_secret(secret_name: str, new_value: str, project_id: str):
    """Updates a secret in Google Secret Manager with a new value."""
    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_id}/secrets/{secret_name}"

    # Add new version with updated value
    client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": new_value.encode("UTF-8")}
        }
    )
    print(f"🔐 Updated cloud secret: {secret_name}")

####################################################################

def update_secrets_file(access_token, refresh_token, client_id, client_secret):
    if is_running_in_cloud():
        print("Updating secrets in Google Secret Manager...")
        # Update secrets in Google Secret Manager
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or "mystrava-464501"
        update_cloud_secret("SOURCES__STRAVA__ACCESS_TOKEN", access_token, project_id)
        update_cloud_secret("SOURCES__STRAVA__REFRESH_TOKEN", refresh_token, project_id)
        # Optional: update client_id/client_secret if needed
    else:
        print("writing to local file ./dlt/temp_secret.toml")
        """Updates the DLT secrets file with the new tokens."""
        temp_secret_path = ".dlt/temp_secret.toml"
    
        lines = [
            "[sources.strava]",
            f'access_token  = "{access_token}"',
            f'refresh_token = "{refresh_token}"',
            f'client_id     = "{client_id}"',
            f'client_secret = "{client_secret}"',
            ""
        ]
        
        os.makedirs(os.path.dirname(temp_secret_path), exist_ok=True)
        with open(temp_secret_path, "w") as f:
            f.write("\n".join(lines))
        
        print(f"✅ Updated {temp_secret_path} with new credentials.")
        

if __name__ == "__main__":
    success = refresh_access_token()
    if success:
        print("🎉 Token refresh successful! You can now run your pipeline.")
    else:
        print("💥 Token refresh failed. You may need to re-authorize.")


### below was old method, which used to just write on temp_secrets.toml , the one in use above has if condition to check either cloud vairables 

# def update_secrets_file(access_token, refresh_token, client_id, client_secret):
#     """Updates the DLT secrets file with the new tokens."""
#     temp_secret_path = ".dlt/temp_secret.toml"
    
#     lines = [
#         "[sources.strava]",
#         f'access_token  = "{access_token}"',
#         f'refresh_token = "{refresh_token}"',
#         f'client_id     = "{client_id}"',
#         f'client_secret = "{client_secret}"',
#         ""
#     ]
    
#     os.makedirs(os.path.dirname(temp_secret_path), exist_ok=True)
#     with open(temp_secret_path, "w") as f:
#         f.write("\n".join(lines))
    
#     print(f"✅ Updated {temp_secret_path} with new credentials.")
