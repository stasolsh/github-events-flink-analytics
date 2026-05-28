package com.example.github.flink;

public class GithubEventMapper {

    public EnrichedGithubEvent enrich(GithubEvent event) {
        String severity = switch (event.getType()) {
            case "PullRequestEvent", "IssuesEvent" -> "HIGH";
            case "PushEvent" -> "MEDIUM";
            default -> "LOW";
        };

        return new EnrichedGithubEvent(
                event.getEventId(),
                event.getType(),
                event.getRepo(),
                event.getActor(),
                event.getCreatedAt(),
                severity,
                System.currentTimeMillis()
        );
    }
}
