package org.jackyzeng.demos.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehaviour {
    private long userId;
    private long itemId;
    private int categoryId;
    private String behaviour;
    private long timestamp;
}
