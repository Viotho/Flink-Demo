package org.jackyzeng.demos.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserBehaviour {
    private long userId;
    private long itemId;
    private int categoryId;
    private String behaviour;
    private long timestamp;
}
