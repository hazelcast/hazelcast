package com.hazelcast.client;

/**
 * @author mdogan 7/12/13
 */
final class ListenerRegistration {

    final String service;

    final String topic;

    final String id;

    ListenerRegistration(String service, String topic, String id) {
        this.service = service;
        this.topic = topic;
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ListenerRegistration that = (ListenerRegistration) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (service != null ? !service.equals(that.service) : that.service != null) return false;
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = service != null ? service.hashCode() : 0;
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ListenerRegistration{");
        sb.append("service='").append(service).append('\'');
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
