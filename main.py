import os

from app.api import create_app

app = create_app(os.environ.get("OSS_ROOT", "./data"))
