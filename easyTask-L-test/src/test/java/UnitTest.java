import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.util.DateUtils;
import io.netty.channel.Channel;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

public class UnitTest {
    @Test
    public void test(){
        ZonedDateTime df=DateUtils.parse("2020-05-30 18:55:01");
    }

}

