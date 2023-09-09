package org.luncert.javaexamples.archivedatabase.export.repo;

import org.luncert.javaexamples.archivedatabase.export.model.Transmission;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ITransmissionRepo extends JpaRepository<Transmission, Long> {
}
