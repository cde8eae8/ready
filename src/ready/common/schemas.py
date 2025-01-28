from marshmallow import Schema, fields

class Server(Schema):
  id = fields.Str()

class App(Schema):
  name = fields.Str()
  version = fields.Str()

class Run(Schema):
  id = fields.UUID()
  server = fields.Nested(Server())
  app = fields.Nested(App())

class RegularExpectation(Schema):
  name = fields.Str()
  time = fields.AwareDateTime()
  valid_for = fields.TimeDelta()

class PingSchema(Schema):
  run = fields.Nested(Run())
  ping = fields.Nested(RegularExpectation())