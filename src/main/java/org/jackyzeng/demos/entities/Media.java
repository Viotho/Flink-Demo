package org.jackyzeng.demos.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Media {
    private String symbol;
    private long timestamp;
    private String Status;
}
