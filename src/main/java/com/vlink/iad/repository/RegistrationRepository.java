package com.vlink.iad.repository;

import com.vlink.iad.entity.RegistrationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface RegistrationRepository extends JpaRepository<RegistrationEntity, Long> {
    // Check if a Registration exists
    @Query("SELECT COUNT(c) > 0 FROM RegistrationEntity c WHERE c.classId = :classId and c.userName = :userName and c.registrationDate = :registrationDate")
    boolean existsByRegistration(String classId, String userName, String registrationDate);

    // Check if a Class and UserName exist
    @Query("SELECT COUNT(c) > 0 FROM RegistrationEntity c WHERE c.classId = :classId and c.userName = :userName")
    boolean existsByClassAndUserName(String classId, String userName);

}
