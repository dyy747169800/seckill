package com.example.seckill.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * @author 段杨宇
 * @create 2019-04-20 11:07
 **/
@Controller
public class DemoController {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static String scriptSha;

    private static final String PRODUCT = "product";

    /**
     * 商品总数
     */
    private static final String TOTAL = "200";

    /**
     * 已购买数
     */
    private static final String BOOKED = "0";

    /**
     * 每次购买数量
     */
    private static final String BUY_COUNT = "1";

    /**
     * 购买成功的用户id 并发会有线程安全问题，这里包装一下变为线程安全的
     */
    private static final List<String> IDS = Collections.synchronizedList(new ArrayList<>());

    @RequestMapping("/index")
    public String index(){
        return "index";
    }

    /**
     * 开始抢购
     * @return
     */
    @RequestMapping("/buy")
    @ResponseBody
    public boolean kill(){
        return redisKill();
    }

    /**
     * 查看成功抢购的用户id
     * @return
     */
    @RequestMapping("/ids")
    @ResponseBody
    public List<String> surplus(){
        return IDS;
    }


    private boolean redisKill() {
        String id = UUID.randomUUID().toString();
        Long execute = redisTemplate.execute((RedisCallback<Long>) connection -> connection.evalSha(scriptSha, ReturnType.INTEGER, 1, PRODUCT.getBytes(),BUY_COUNT.getBytes()));
        boolean buySuccess = Objects.equals(BUY_COUNT, String.valueOf(execute));
        if(buySuccess){
           successKill(id);
        }
        return buySuccess;
    }

    /**
     * 抢购成功 这里可以写入消息队列，进行消费 ，后端入库处理
     * @param id
     */
    private void successKill(String id) {
        IDS.add(id);
    }


    @PostConstruct
    public void init(){
        Map<String, String> product = new HashMap<>();
        product.put("total",TOTAL);
        product.put("booked",BOOKED);
        redisTemplate.opsForHash().putAll(PRODUCT,product);
        String luaScript = "local n = tonumber(ARGV[1]) " +
                            "if not n or n == 0 then " +
                            "return 0 " +
                            "end " +
                            "local vals = redis.call('HMGET', KEYS[1], 'total', 'booked'); " +
                            "local total = tonumber(vals[1]) " +
                            "local blocked = tonumber(vals[2]) " +
                            "if not total or not blocked then " +
                            "return 0 " +
                            "end " +
                            "if blocked + n <= total then " +
                            "redis.call('HINCRBY', KEYS[1], 'booked', n) " +
                            "return n; " +
                            "end " +
                            "return 0";
        //加载脚本,也可以不加载脚本,每次eval脚本执行
        scriptSha = redisTemplate.execute((RedisCallback<String>) connection -> connection.scriptLoad(luaScript.getBytes()));
    }
}
