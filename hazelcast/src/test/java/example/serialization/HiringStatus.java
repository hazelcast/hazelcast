package example.serialization;

public enum HiringStatus {

    HIRING ("Hiring"),
    NOT_HIRING ("Not hiring");

    private final String humanReadable;

    HiringStatus(String humanReadable) {
        this.humanReadable = humanReadable;
    }
}
