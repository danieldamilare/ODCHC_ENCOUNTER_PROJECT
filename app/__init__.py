from flask import Flask
from app.config import Config
from flask_login import LoginManager

app = Flask(__name__)
app.config.from_object(Config)
login = LoginManager(app)
login.login_view = 'login'
login.login_message = "Please login to access system"
