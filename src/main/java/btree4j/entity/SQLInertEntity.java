/*
 * @Author: LHD
 * @Date: 2024-09-10 14:21:13
 * @LastEditors: 308twin 790816436@qq.com
 * @LastEditTime: 2024-09-10 14:31:22
 * @Description: 
 * 
 * Copyright (c) 2024 by 308twin@790816436@qq.com, All Rights Reserved. 
 */
package btree4j.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SQLInertEntity implements java.io.Serializable {
    private String dbName;
    private String tableName;
    private String sql;
}
