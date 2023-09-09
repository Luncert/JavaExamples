package org.luncert.javaexamples.archivedatabase.export.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
public class Transmission implements Serializable {

    private static final long serialVersionUID = 8391967168503176920L;

    @Id
    private Long id;

    private String name;

    private String payload;
}
