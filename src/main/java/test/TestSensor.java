package test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor

public class TestSensor {
    private String id;
    private Long ts;
    private Integer vc;
    private Integer increaseId;
}
