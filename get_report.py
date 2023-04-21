# Online Python compiler (interpreter) to run Python online.
# Write Python 3 code in this online editor and run it.
import requests

url = "https://b2b.taxi.yandex.net/b2b/cargo/integration/v2/claims/search"
date_from = "2023-04-16"
date_to = "2023-04-19"
timezone_offset = "-05:00"
payload = json.dumps({
    "created_from": f"{date_from}T00:00:00{timezone_offset}",
    "created_to": f"{date_to}T23:59:59{timezone_offset}",
    "limit": 1000,
    "cursor": 0
})

client_secret = "y0_AgAAAABpCSWQAAc6MQAAAADecJWo-54QWQeXTcmlC8Qm4hsM5i4Ddtk"

headers = {
    'Content-Type': 'application/json',
    'Accept-Language': 'en',
    'Authorization': f"Bearer {client_secret}"
}

response = requests.request("POST", url, headers=headers, data=payload)
print(response)
