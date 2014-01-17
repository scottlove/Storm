package DataOutputter;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import io.netty.handler.codec.string.StringEncoder;


public class TCPMessageSender {


    private final int port;
    private final String host;
    private Logger logger ;
    Bootstrap b;
    EventLoopGroup group;


    public void close() throws Exception
    {

        group.shutdownGracefully().sync();
    }

    public TCPMessageSender(int port, String host)  {

        this.host = host;
        this.port = port;
        this.logger = LogManager.getLogger(TCPMessageSender.class.getName());

        group = new NioEventLoopGroup();
        try {


            b = new Bootstrap();

            b.group(group)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(new InetSocketAddress(host, port))
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {


                            ch.pipeline().addLast("encoder",new StringEncoder() );

                        }
                    });
        }
        catch (Exception e)
        {
            logger.error(e.toString());
        }


    }
    public void sendMessage( String message)
    {


        try {


            Channel channel = b.connect(host, port).sync().channel();


            channel.write(message + "\r\n");
            channel.flush();

            channel.close();




        }
        catch (Exception e)
        {
            logger.error(e.toString());
        }
    }
}


