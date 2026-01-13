from flask import Flask
from app.config import Config
from flask_login import LoginManager

app = Flask(__name__)
app.config.from_object(Config)
login = LoginManager(app)
login.login_view = 'login'
login.login_message = "Please login to access system"

from app.commands import run_test_command
from app.db import init_db_command, seed_db

app.cli.add_command(init_db_command)
app.cli.add_command(seed_db)
app.cli.add_command(run_test_command)

from app import routes, services, models
from jinja2 import StrictUndefined

app.jinja_env.undefined = StrictUndefined
