from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_login import LoginManager
from flask_mail import Mail
from autocompy.config import Config
import logging


logging.basicConfig(format="%(asctime)s: %(name)s: %(levelname)s:: %(message)s", filename='autocompy/logs/logs.log',
                    encoding='utf-8', level=logging.INFO)
log = logging.getLogger("AutoCompy")

db = SQLAlchemy()
bcrypt = Bcrypt()
login_manager = LoginManager()
login_manager.login_view = 'users.login'
login_manager.login_message_category = 'info'
mail = Mail()


def create_app(config_class=Config):

    from autocompy.users.routes import users
    from autocompy.main.routes import main
    from autocompy.errors.handlers import errors

    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)
    bcrypt.init_app(app)
    login_manager.init_app(app)
    mail.init_app(app)

    app.register_blueprint(users)
    app.register_blueprint(main)
    app.register_blueprint(errors)

    return app
