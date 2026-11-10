"""Read browser status from the kiosk's local ChromeDriver sessions."""
import base64
import json
from pathlib import Path
from urllib.request import Request, urlopen

for entry in Path("/proc").iterdir():
    if not entry.name.isdigit():
        continue
    try:
        if entry.joinpath("comm").read_text().strip() != "chromedriver":
            continue
        args = entry.joinpath("cmdline").read_bytes().decode().split("\0")
        port = next(arg.split("=", 1)[1] for arg in args if arg.startswith("--port="))
    except (OSError, StopIteration):
        continue

    def call(path, payload=None):
        data = None if payload is None else json.dumps(payload).encode()
        request = Request(f"http://127.0.0.1:{port}" + path, data=data,
                          headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=20) as response:
            return json.load(response)["value"]

    for session in call("/sessions"):
        prefix = "/session/" + session["id"]
        url = call(prefix + "/url")
        side = "left" if "visualize.energy" in url else "right"
        result = call(prefix + "/execute/sync", {
            "script": "return {title: document.title, loading: document.body.innerText.includes('Loading energy data'), text: document.body.innerText.slice(0,1500)};",
            "args": [],
        })
        print(json.dumps({"window": side, "url": url,
                          "rect": call(prefix + "/window/rect"), "page": result}), flush=True)
        screenshot = call(prefix + "/screenshot")
        (Path.home() / "kiosk" / f"smoke-{side}.png").write_bytes(base64.b64decode(screenshot))
