import com.github.liuche51.easyTask.util.DateUtils;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class UnitTest {
    public static List<Thread> threadList=new LinkedList<>();
    @Test
    public void test() throws InterruptedException {
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                     if(Thread.currentThread().isInterrupted()){
                         System.out.println("线程中断");
                         break;
                     }else {
                         System.out.println("线程运行中");

                     }
                }
            }
        });
        th1.start();
        threadList.add(th1);
        Thread.sleep(5000);
        threadList.forEach(x->{
            x.interrupt();
        });
        Thread.sleep(5000);
    }

}

