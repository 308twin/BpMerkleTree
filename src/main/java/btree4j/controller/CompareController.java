package btree4j.controller;

import org.springframework.web.bind.annotation.RestController;

import btree4j.service.CompareService;
import btree4j.server.SignatureService;

import java.security.PublicKey;
import java.util.Base64;

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
        try {
            PublicKey publicKey = signatureService.getPublicKey(tableName);
            return Base64.getEncoder().encodeToString(publicKey.getEncoded());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
}
