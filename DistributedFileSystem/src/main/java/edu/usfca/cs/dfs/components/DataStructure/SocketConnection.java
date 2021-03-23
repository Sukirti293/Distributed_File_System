package edu.usfca.cs.dfs.components.DataStructure;

import java.io.IOException;
import java.net.Socket;
import java.util.Objects;

public class SocketConnection implements Comparable<SocketConnection> {
    private final String host;
    private final int port;

    public SocketConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return host + ":" + port;
    }

    public Socket getSocket() throws IOException {
        return new Socket(host, port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SocketConnection that = (SocketConnection) o;
        return port == that.port &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public int compareTo(SocketConnection o) {
        if (!this.host.equals(o.host)) {
            return this.host.compareTo(o.host);
        }
        return Integer.compare(this.port, o.port);
    }


}
