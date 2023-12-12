from flask import Flask
def create_app():
    app = Flask(__name__)
    app.secret_key = 'Quadri Basit Ayomide'
    
    from .app import load
    
    app.register_blueprint(load,url_prefix='/')

    return app