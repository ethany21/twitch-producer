package com.github.ethany.kafka.twitchproducer.kafka.util;

import lombok.Builder;
import lombok.Data;

import java.io.OutputStream;
import java.net.Socket;

@Builder
@Data
public class SocketOutputStreamDto {
    private Socket socket;
    private OutputStream outputStream;
}
