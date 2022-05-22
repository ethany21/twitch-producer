package com.github.ethany.kafka.twitchproducer.kafka.util;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

@RequiredArgsConstructor
@Builder
public class TwitchIrcConnection implements Runnable {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    private final String channel;
    private final Logger logger;

    public void run() {

        while (true) {
            SocketOutputStreamDto streamDto = null;
            try {
                streamDto = CreateSocketOutputStream.builder().logger(logger).build().createSocketOutputStream();
                streamDto.getOutputStream().write(("JOIN #" + channel + "\n").getBytes(StandardCharsets.UTF_8));
                logger.info("check if socket Connected: " + streamDto.getSocket().isConnected());
                readMessageFromOutputStream(streamDto);
            } catch (IOException e) {
                logger.info("cannot connect to " + channel + ", reconnection");
            } finally {
                try {
                    assert streamDto != null;
                    streamDto.getSocket().close();
                    streamDto.getOutputStream().close();
                } catch (IOException e) {
                    logger.info(e.getMessage());
                }
            }
        }
    }


    public void readMessageFromOutputStream(SocketOutputStreamDto streamDto) throws IOException {
        String line;
        InputStreamReader inputStreamReader = new InputStreamReader(streamDto.getSocket().getInputStream(), StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        while ((line = bufferedReader.readLine()) != null) {  /* SocketException: Connection Reset occurs on this line*/
            catchUnexpectedMessage(line, streamDto);
        }
    }

    public void catchUnexpectedMessage(String line, SocketOutputStreamDto streamDto) throws IOException {
        try {
            kafkaOrIrcMultiplexer(line, streamDto);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.info("I Caught exception " + e.getMessage() + " which is : " + line);
        }
    }

    public void kafkaOrIrcMultiplexer(String line, SocketOutputStreamDto streamDto) throws IOException {
        if (line.split(" ")[0].equals("PING")) {
            streamDto.getOutputStream().write(("PONG\n").getBytes(StandardCharsets.UTF_8));
        } else {
            String key = line.split(" ")[2];
            kafkaTemplate.send(topic, key, line);
        }
    }

}
