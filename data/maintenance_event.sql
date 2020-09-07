CREATE TABLE "maintenance_event" ("id" integer,
  "event_date" text,
  "event_type" text,
  "bike_id" int,
  "details" text,
  PRIMARY KEY ("id"),
  FOREIGN KEY("bike_id") REFERENCES bike("id"))
