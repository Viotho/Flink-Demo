package org.jackyzeng.demos.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class StockPrice {
    private String symbol;
    private Long timestamp;
    private Double price;
    private Integer volume;
    private String mediaStatus;
}
