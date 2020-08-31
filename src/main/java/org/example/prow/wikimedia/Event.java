package org.example.prow.wikimedia;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;


public class Event {
    final static DateTimeFormatter fmter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S").withZone(ZoneOffset.UTC);

    public String wikiDB;
    public EventEntity eventEntity;
    public EventType eventType;
    public ZonedDateTime eventTimestamp;
    public String rest;
    public Long something = 1L;

    public Event() {

    }

    @Override
    public String toString() {
        return "Event{" +
                "wikiDB='" + wikiDB + '\'' +
                ", eventEntity=" + eventEntity +
                ", eventType=" + eventType +
                ", eventTimestamp=" + eventTimestamp +
                ", rest='" + rest + '\'' +
                ", something=" + something +
                '}';
    }

    public Event(String wikiDB, EventEntity eventEntity, EventType eventType, ZonedDateTime eventTimestamp, String rest) {
        this.wikiDB = wikiDB;
        this.eventEntity = eventEntity;
        this.eventType = eventType;
        this.eventTimestamp = eventTimestamp;
        this.rest = rest;
    }

    public Event(String line) throws Exception {
        final Scanner scanner = new Scanner(line).useDelimiter("\t");

        this.wikiDB = scanner.next();
        this.eventEntity = EventEntity.fromString(scanner.next());
        this.eventType = EventType.fromString(scanner.next());

        this.eventTimestamp = ZonedDateTime.parse(scanner.next(), fmter);
        this.rest = scanner.nextLine();
    }

}
