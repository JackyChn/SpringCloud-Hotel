package cn.itcast.hotel.service;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParam;
import com.baomidou.mybatisplus.extension.service.IService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IHotelService extends IService<Hotel> {

    PageResult search(RequestParam requestParam) throws IOException;

    Map<String, List<String>> filters(RequestParam requestParam) throws IOException;

    List<String> getSuggestions(String prefix) throws IOException;

    void insertById(Long id) throws IOException;

    void deleteById(Long id);
}
