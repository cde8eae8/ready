from flask import Flask, request
import flask
import marshmallow
from ready.common import schemas

def validate_body(schema: marshmallow.Schema):
  def _decorator(f):
    def _impl(*args, **kwargs):
      try:
        body = schema.load(request.body)
      except marshmallow.ValidationError as err:
        # TODO:
        flask.abort()
      assert "body" not in kwargs
      kwargs["body"] = body
      f(*args, **kwargs)
    return _impl
  return _decorator

app = Flask(__name__)


@app.route("/ping")
@validate_body(schemas.PingSchema())
def ping(body: schemas.PingSchema):
  print(body)