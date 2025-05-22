package com.vlink.iad.repository;

import com.vlink.iad.entity.OfferingEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.Optional;
import java.util.List;

@Repository
public interface OfferingRepository extends JpaRepository<OfferingEntity, Long> {
    // Check if a Offering exists
    @Query("SELECT COUNT(c) > 0 FROM OfferingEntity c WHERE c.courseId = :courseId and c.classId = :classId and c.courseExternalCourseId = :courseExternalCourseId")
    boolean existsByOffering(String courseId, String courseExternalCourseId, String classId);

	// Find ExternalCourseId when provide CourseId
	@Query("SELECT e.courseExternalCourseId FROM OfferingEntity e WHERE e.courseId = :courseId")
	List<String> getAllCourseExternalCourseIdsByCourseId(@Param("courseId") String courseId);	

	// Find CourseId when provide ExternalCourseId
	@Query("SELECT e.courseId FROM OfferingEntity e WHERE e.courseExternalCourseId = :courseExternalCourseId")
	List<String> getAllCourseIdsByCourseExternalCourseId(@Param("courseExternalCourseId") String courseExternalCourseId);	
	
	// Check if a Class exists
    @Query("SELECT COUNT(c) > 0 FROM OfferingEntity c WHERE c.classId = :classId")
    boolean existsByClass(String classId);
	
    // Query method to find all matching records
    List<OfferingEntity> findByCourseExternalCourseId(String courseExternalCourseId);


	
}
