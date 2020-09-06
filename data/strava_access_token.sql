CREATE TABLE "strava_access_token" (
	"id"	INTEGER,
	"athlete_id"	INTEGER NOT NULL,
	"access_token"	TEXT NOT NULL,
	"expires_at"	INTEGER NOT NULL,
	"refresh_token"	TEXT NOT NULL,
	"created_at"	TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	"modified_at"	TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("id")
)
