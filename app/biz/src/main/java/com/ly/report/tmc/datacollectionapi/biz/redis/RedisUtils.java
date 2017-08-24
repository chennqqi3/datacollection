package com.ly.report.tmc.datacollectionapi.biz.redis;

import com.ly.sof.utils.log.LoggerUtils;
import com.ly.sof.utils.mapping.FastJsonUtils;

import com.ly.tcbase.cacheclient.CacheClientHA;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * redis工具类
 * Created by hyz46086 on 2017/4/20.
 *
 */
public class RedisUtils {

    /** logger */
    private static Logger        logger      = LoggerFactory.getLogger(RedisUtils.class);

    /** ly redis client */
    private static CacheClientHA redisClient = new CacheClientHA("flight.datacollectionapi", true);

    /**
     * 获取缓存的list大小
     * @param key
     * @return
     */
    public static long GetListLength(String key) {

        return redisClient.List().llen(key);
    }

    /**
     * 
     * @param key
     * @param start
     * @param end
     * @param t
     * @return
     */
    public static <T> List<T> GetRangeObjFromList(String key, long start, long end, Class<T> t) {

        List<String> jsonList = redisClient.List().lrange(key, start, end);
        List<T> listObj = new ArrayList<T>();
        for (String jsonstr : jsonList) {
            T obj = FastJsonUtils.fromJSONString(jsonstr, t);
            if (obj != null) {
                listObj.add(obj);
            }
        }
        return listObj;
    }

    /**
     * set value
     * @param key
     * @param value
     * @return
     */
    public static boolean set(String key, String value, int seconds) {
        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(value)) {
                redisClient.String().set(key, value);
                redisClient.Key().expire(key, seconds);
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "添加key错误", ex);
        } finally {

        }
        return false;
    }

    /**
     * 添加锁
     * @param key
     * @param value
     * @param second
     * @return
     */
    public boolean setnx(String key, String value, int second) {
        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(value)) {
                boolean acquired = redisClient.String().setnx(key, value);
                if (acquired == true) {
                    //获取锁成功
                    if (second > 0)
                        redisClient.Key().expire(key, second);
                    return true;
                }
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "添加锁错误", ex);
        } finally {

        }
        return false;
    }

    /**
     * 获取key old值
     * @param key
     * @param value
     * @return
     */
    public String getset(String key, String value) {
        String result = null;
        try {
            result = redisClient.String().getset(key, value);
        } catch (Exception e) {
            LoggerUtils.error(logger, "getset 获取值", e);
        } finally {

        }
        return result;
    }

    /**
     * 删除key
     * @param key
     * @return
     */
    public static boolean del(String key) {
        try {
            boolean res = false;
            if (null != key) {
                if (isExistsString(key)) {
                    res = redisClient.Key().del(key);
                } else {
                    res = true;
                }
                return res;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "删除key错误", ex);
        } finally {

        }
        return false;
    }

    /**
     * 添加json字符设置过期时间
     * @param key
     * @param value
     * @param seconds
     * @return
     */
    public boolean addJsonValue(String key, Object value, int seconds) {
        try {
            if (!StringUtils.isBlank(key) && null != value) {
                redisClient.String().set(key, FastJsonUtils.toJSONString(value));
                if (seconds > 0) {
                    redisClient.Key().expire(key, seconds);
                }
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "添加key错误", ex);
        }
        return false;
    }

    /**
     * 添加字符串并设置过期时间
     * @param key
     * @param value
     * @param seconds
     * @return
     */
    public boolean setValue(String key, String value, int seconds) {
        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(value)) {
                redisClient.String().set(key, value);
                if (seconds > 0) {
                    redisClient.Key().expire(key, seconds);
                }
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "添加key错误", ex);
        }
        return false;
    }

    /**
     * 获取字符串
     * @param key
     * @return
     */
    public static String getValue(String key) {

        try {
            return redisClient.String().get(key);
        } catch (Exception ex) {
            LoggerUtils.error(logger, "getValue错误", ex);
        }
        return null;
    }

    /**
     * 设置某个key过期
     * 
     * @param key
     * @param seconds
     * @return
     */
    public boolean expired(String key, int seconds) {

        try {
            if (!StringUtils.isBlank(key) && seconds > 0) {
                return redisClient.Key().expire(key, seconds);
            }
        } catch (Exception e) {
            LoggerUtils.error(logger, "expired错误", e);
        }
        return false;
    }

    /**
     * 判断key是否存在
     * 
     * @param key
     * @return
     */
    public static boolean isExistsString(String key) {
        try {
            if (null != key) {
                return redisClient.Key().exists(key);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "isExists错误", ex);
        }
        return false;
    }

    /**
     * 判断hash中的key是否存在
     * 
     * @param key
     * @return
     */
    public boolean isHashKeyExists(String key, String field) {
        try {
            if (null != key) {
                return redisClient.Hash().hexists(key, field);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "isHashKeyExists错误2", ex);
        }
        return false;
    }

    /**
     * 自增
     * @param key
     * @return
     */
    public boolean incrKey(String key) {

        try {
            if (!StringUtils.isBlank(key)) {
                redisClient.String().incrby(key, 1);
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "incrKey错误", ex);
        }
        return false;
    }

    /**
     * 增加指定的value
     * @param key
     * @param value
     * @return
     */
    public static boolean incrKey(String key, long value) {

        try {
            if (!StringUtils.isBlank(key) && value > 0) {
                redisClient.String().incrby(key, value);
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "incrKey错误", ex);
        }
        return false;
    }

    public  static boolean decrKey(String key, int value) {

        try {
            if (!StringUtils.isBlank(key) && value > 0) {
                long result = redisClient.String().decrby(key, value);
                if (result < 0) {
                    return false;
                }
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "decrKey错误", ex);
        }
        return false;
    }

    /**
     * 添加list
     * @param key
     * @param value
     * @return
     */
    public static boolean addList(String key, String value) {

        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(value)) {
                redisClient.List().lpush(key, value);
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addList错误", ex);
        }
        return false;
    }

    /**添加set
     * @param key
     * @param value
     * @return
     */
    public boolean addSet(String key, String value) {

        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(value)) {
                redisClient.Set().sadd(key, value);
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addSet错误", ex);
        }
        return false;
    }

    /**
     * 返回set
     * @param key
     * @param value
     * @return
     */
    public Set<String> getSet(String key, String value) {

        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(value)) {
                return redisClient.Set().smembers(key);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "getset错误", ex);
        }
        return null;
    }

    /**
     * 取出set中的值
     * @param key
     * @return
     */
    public String popSet(String key) {

        try {
            if (!StringUtils.isBlank(key)) {
                return redisClient.Set().spop(key);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "popset错误", ex);
        }
        return null;
    }

    /**
     * 添加hash唯一
     * @param key
     * @param field
     * @param value
     * @return
     */
    public static boolean addHashnx(String key, String field, String value) {

        try {
            if (!redisClient.Key().exists(key)) {
                redisClient.Key().expire(key, 31104000);
            }
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(field) && !StringUtils.isBlank(value)) {
                return redisClient.Hash().hsetnx(key, field, value);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addHashnx错误", ex);
        }
        return false;
    }

    /**
     * 添加hash
     * @param key
     * @param field
     * @param value
     * @return
     */
    public boolean addHash(String key, String field, String value) {

        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(field) && !StringUtils.isBlank(value)) {
                return redisClient.Hash().hset(key, field, value);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addHash错误", ex);
        }
        return false;
    }

    /**
     * 删除hash
     * @param key
     * @param field
     * @return
     */
    public boolean removeHash(String key, String field) {
        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(field)) {
                long result = redisClient.Hash().hdel(key, field);
                if (result == 1) {
                    return true;
                }
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addSet错误", ex);
        }
        return false;
    }

    /**
     * 获取hash
     * @param key
     * @param field
     * @return
     */
    public static String getHash(String key, String field) {

        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(field)) {
                return redisClient.Hash().hget(key, field);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "getHash错误", ex);
        }
        return null;
    }

    /**
     * 
     * @param key
     * @param value
     * @param seconds
     * @return
     */
    public static boolean addHashAll(String key, Map<String, String> value, int seconds) {
        try {
            if (!StringUtils.isBlank(key) && MapUtils.isNotEmpty(value)) {
                return (redisClient.Hash().hmset(key, value) && redisClient.Key().expire(key, seconds));
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addHashAll错误", ex);
        }
        return false;

    }

    /**
     * 删除Map指定key的值
     * @param key
     * @param field
     * @return
     */
    public static boolean delHash(String key, String... field) {
        try {
            if (!StringUtils.isBlank(key) && field.length > 0) {
                return redisClient.Hash().hdel(key, field) > 0 ? true : false;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addHashAll错误", ex);
        }
        return false;

    }

    /**
     * 获取所有hash
     * @param key
     * @return
     */
    public static Map<String, String> getHashAll(String key) {

        try {
            if (!StringUtils.isBlank(key)) {
                return redisClient.Hash().hgetall(key);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "getHashAll错误", ex);
        }
        return null;
    }

    /**
     * 获取所有hash Map&lt;String, byte[]>结构
     * 
     * @param key
     * @return
     */
    public Map<String, byte[]> getHashBitAll(String key) {

        try {
            if (!StringUtils.isBlank(key)) {
                return redisClient.Hash().hgetallBit(key);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "getHashBitAll错误", ex);
        }
        return null;
    }

    /**
     * 添加list 设置过期时间
     * @param key
     * @param value
     * @param seconds
     * @return
     */
    public static boolean addList(String key, String value, int seconds) {
        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(value)) {
                redisClient.List().lpush(key, value);
                redisClient.Key().expire(key, seconds);
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addList错误", ex);
        }
        return false;
    }

    /**
     * 将list存入redis 
     * @param list list对象
     * @param key key值
     * @param expireTime 过期时间 （秒）
     */
    public static <T> boolean PushItemObjToList(String key, List<T> list, int expireTime) {
        try {
            for (T item : list) {
                redisClient.List().rpush(key, FastJsonUtils.toJSONString(item));
            }
            redisClient.Key().expire(key, expireTime);
            return true;
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addList错误", ex);
        }
        return false;
    }

    /**
     * 返回列表 key 的长度。<br/>
     * 如果 key 不存在，则 key 被解释为一个空列表，返回 0 .<br/>
     * 如果 key 不是列表类型，返回一个错误。
     * @param key
     * @return
     */
    public Long llen(String key) {
        try {
            if (!StringUtils.isBlank(key)) {
                Long llen = redisClient.List().llen(key);
                return llen;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addList错误", ex);
        }
        return 0L;
    }

    /**
     * 添加list
     * @param key
     * @param values
     * @return
     */
    public boolean addList(String key, List<String> values) {

        try {
            if (!StringUtils.isBlank(key) && null != values && values.size() > 0) {

                for (String value : values) {
                    redisClient.List().lpush(key, value);
                }
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addList错误", ex);
        }
        return false;
    }

    /**
     * 添加list 设置过期时间
     * @param key
     * @param values
     * @param seconds
     * @return
     */
    public boolean addList(String key, List<String> values, int seconds) {

        try {
            if (!StringUtils.isBlank(key) && null != values && values.size() > 0) {

                for (String value : values) {
                    redisClient.List().lpush(key, value);
                }
                redisClient.Key().expire(key, seconds);
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addList错误", ex);
        }
        return false;
    }

    /**
     * 获取list
     * @param key
     * @return
     */
    public List<String> getList(String key) {

        try {
            if (!StringUtils.isBlank(key)) {
                return redisClient.List().lrange(key, 0, -1);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "getList 错误", ex);

        }
        return null;
    }

    /**
     * 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 stop 指定<br/>
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。<br/>
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。<br/>
     * 注意LRANGE命令和编程语言区间函数的区别，返回结果包含stop 下标所在元素<br/>
     * 超出范围的下标值不会引起错误。<br/>
     * 如果 start 下标比列表的最大下标 end ( LLEN list 减去 1 )还要大，那么 LRANGE 返回一个空列表。<br/>
     * 如果 stop 下标比 end 下标还要大，Redis将 stop 的值设置为 end 。
     * 
     * @param key List的key
     * @param start 起始位置
     * @param stop 结束位置
     * @return
     */
    public List<String> lrange(String key, int start, int stop) {

        try {
            if (!StringUtils.isBlank(key)) {
                return redisClient.List().lrange(key, start, stop);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "lrange错误2", ex);

        }
        return null;
    }

    /**
     * 添加byte 设置过期
     * @param key
     * @param object
     * @param seconds
     * @return
     */
    public boolean addValueSerialize(String key, Object object, int seconds) {
        try {
            if (!StringUtils.isBlank(key) && null != object) {
                byte[] bytes = serialize(object);
                if (bytes != null) {
                    redisClient.String().setBit(key, bytes);
                    redisClient.Key().expire(key, seconds);
                }
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addValueSerialize错误", ex);
        }
        return false;
    }

    /**
     * 添加byte
     * @param key
     * @param object
     * @return
     */
    public boolean addValueSerialize(String key, Object object) {
        try {
            if (!StringUtils.isBlank(key) && null != object) {
                byte[] bytes = serialize(object);
                if (bytes != null) {
                    redisClient.String().setBit(key, bytes);
                }
                return true;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addValueSerialize错误", ex);
        }
        return false;
    }

    /**
     * 添加hash byte
     * @param key
     * @param object
     * @return
     */
    public boolean addHashValueSerialize(String key, String field, Object object) {
        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(field) && null != object) {
                byte[] bytes = serialize(object);
                if (bytes != null) {
                    return redisClient.Hash().hsetBit(key, field, bytes);
                }
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "addHashValueSerialize错误2", ex);
        }
        return false;
    }

    /**
     * 将对象转为json字符串后存储
     * @param key
     * @param object
     * @return
     */
    public boolean haddObjectToJson(String key, String field, Object object) {
        try {
            if (!StringUtils.isBlank(key) && !StringUtils.isBlank(field) && null != object) {
                String jsonStr = FastJsonUtils.toJSONString(object);
                if (jsonStr != null && jsonStr.length() > 0) {
                    return redisClient.Hash().hset(key, field, jsonStr);
                }
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "haddObjectToJson错误2", ex);
        }
        return false;
    }

    /**
     * 获取object
     * @param key
     * @return
     */
    public Object getValueDerialize(String key) {
        try {
            if (null != key) {
                byte[] bytes = redisClient.String().getBit(key);
                return derialize(bytes);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "getValueDerialize错误", ex);
        }
        return null;
    }

    /**
     * 获取Hash object
     * @param key
     * @return
     */
    public Object getHashValueDerialize(String key, String field) {
        try {
            if (null != key && StringUtils.isNotBlank(field)) {
                byte[] bytes = redisClient.Hash().hgetBit(key, field);
                return derialize(bytes);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "getHashValueDerialize错误2", ex);
        }
        return null;
    }

    /**
     * 从json字符串中获取Hash object
     * @param key
     * @return
     */
    public <T> T hgetObjectFromJson(String key, String field, Class<T> clazz) {
        try {
            if (null != key && StringUtils.isNotBlank(field)) {
                String jsonStr = redisClient.Hash().hget(key, field);
                return FastJsonUtils.fromJSONString(jsonStr, clazz);
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "hgetObjectFromJson错误2", ex);
        }
        return null;
    }

    private byte[] serialize(Object object) {

        ByteArrayOutputStream byteoutstream = null;
        ObjectOutputStream objectoutstream = null;
        try {
            if (null != object) {

                byteoutstream = new ByteArrayOutputStream();
                objectoutstream = new ObjectOutputStream(byteoutstream);
                objectoutstream.writeObject(object);
                //objectoutstream.flush();
                return byteoutstream.toByteArray();
            }

        } catch (Exception ex) {

            LoggerUtils.error(logger, "序列化失败", ex);
        } finally {
            try {
                if (null != objectoutstream) {
                    objectoutstream.close();
                }
                if (null != byteoutstream) {
                    byteoutstream.close();
                }

            } catch (Exception e) {

            }
        }
        return null;
    }

    private Object derialize(byte[] source) {

        ByteArrayInputStream byteinstream = null;
        ObjectInputStream objectinstream = null;
        Object target = null;
        try {
            if (null != source) {
                byteinstream = new ByteArrayInputStream(source);
                objectinstream = new ObjectInputStream(byteinstream);
                target = objectinstream.readObject();
                return target;
            }
        } catch (Exception ex) {
            LoggerUtils.error(logger, "反序列化失败", ex);
        } finally {
            try {
                if (null != byteinstream) {
                    byteinstream.close();
                }
                if (null != objectinstream) {
                    objectinstream.close();
                }
            } catch (Exception e) {

            }
        }
        return null;
    }

}
