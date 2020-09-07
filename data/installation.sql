CREATE TABLE "installation" ("id" integer,
  "bike_id" int,
  "component_id" int,
  "start_date" text,
  "end_date" text,
  PRIMARY KEY ("id"),
  FOREIGN KEY("bike_id") REFERENCES bike("id"),
  FOREIGN KEY("component_id") REFERENCES component("id"))
