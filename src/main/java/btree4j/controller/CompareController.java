package btree4j.controller;

import org.springframework.web.bind.annotation.RestController;

import btree4j.service.CompareService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@RestController
public class CompareController {
    @Autowired
    private CompareService compareService;

    @PostMapping("/api/getCompareResult")
    public Object postMethodName(@RequestParam String channelType, @RequestParam String channelName) {
        return compareService.getCompareResult(channelType, channelName);
    }

}
