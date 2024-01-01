package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParam;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Autowired
    private IHotelService iHotelService;

    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParam requestParam) throws IOException {
        return iHotelService.search(requestParam);
    }

    @PostMapping("/filters")
    public Map<String, List<String>> getFilters(@RequestBody RequestParam requestParam) throws IOException {
        return iHotelService.filters(requestParam);
    }

    @GetMapping("/suggestion")
    public List<String> getSuggestions(@org.springframework.web.bind.annotation.RequestParam("key") String prefix) throws IOException {
        return iHotelService.getSuggestions(prefix);
    }
}
