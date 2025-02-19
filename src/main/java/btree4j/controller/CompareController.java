package btree4j.controller;

import org.springframework.web.bind.annotation.RestController;

import btree4j.service.CompareService;
import btree4j.server.SignatureService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@RestController
public class CompareController {
    @Autowired
    private CompareService compareService;
    
    @Autowired
    private SignatureService signatureService;

    @PostMapping("/api/getCompareResult")
    public Object postMethodName(@RequestParam String channelType, @RequestParam String channelName) {
        return compareService.getCompareResult(channelType, channelName);
    }

    @GetMapping("/api/getPublicKey")
    public String getPublicKey(@RequestParam String tableName) {
        return signatureService.getPublicKeyBase64(tableName);
    }
}
