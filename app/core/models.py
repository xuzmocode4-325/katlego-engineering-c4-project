"""
Database models.
"""
from django.conf import settings
from django.db import models
from django_countries.fields import CountryField
from django.contrib.auth.models import (
    AbstractBaseUser,
    BaseUserManager,
    PermissionsMixin
)

class UserManager(BaseUserManager):
    """Class for creating a user manager"""

    def create_user(self, email, password=None, **extrafields):
        """Create, save and return a new user."""
        if not email:
            raise ValueError(
                'An email address is required for user registration.'
            )
        user = self.model(
            email=self.normalize_email(email),
            **extrafields
        )
        user.set_password(password)
        user.save(using=self._db)

        return user

    def create_superuser(self, email, password):
        user = self.create_user(email, password)
        user.is_staff = True
        user.is_superuser = True
        user.save(using=self._db)

        return user

class User(AbstractBaseUser, PermissionsMixin):
    """Model for custom definition of system user fields"""
    email = models.EmailField(max_length=255, unique=True)
    name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)

    objects = UserManager()

    USERNAME_FIELD = 'email'

class AgeRange(models.Model): 
    age_range = models.CharField()

class Country(models.Model):
    country = CountryField()

class Experience(models.Model):
    experience = models.CharField()

class Track(models.Model):
    track = models.CharField()

class Aim(models.Model):
    aim = models.CharField()

class Referral(models.Model):
    referral = models.CharField()

class HoursAvailable(models.Model):
    hours_available = models.CharField()

class SkillLevel(models.Model):
    skill_level = models.CharField()
    skill_description = models.CharField()

class Student(models.Model):
    student_id = models.CharField(
        max_length=255,
        unique=True,
        db_index=True,
        primary_key=True
    )
    gender = models.CharField()
    age_range = models.ForeignKey(AgeRange, on_delete=models.CASCADE)
    country = models.ForeignKey(Country, on_delete=models.DO_NOTHING)
    experience = models.ForeignKey(Experience, on_delete=models.CASCADE)
    track = models.ForeignKey(Track, on_delete=models.CASCADE)
    referral = models.ForeignKey(Referral, on_delete=models.CASCADE)
    skill_level = models.ForeignKey(SkillLevel, on_delete=models.CASCADE)
    hours_available = models.ForeignKey(HoursAvailable, on_delete=models.CASCADE)

class Motivation(models.Model):
    student = models.ForeignKey(
        Student, 
        on_delete=models.CASCADE, 
        to_field='student_id', 
        db_column='student_id'
    )
    aim = models.ForeignKey(Aim, on_delete=models.CASCADE)
    motivation = models.TextField()

class Registration(models.Model):
    student = models.ForeignKey(
        Student, 
        on_delete=models.CASCADE, 
        to_field='student_id', 
        db_column='student_id'
    )
    date = models.DateField()
    time = models.TimeField()

class Outcomes(models.Model):
    student = models.ForeignKey(
        Student, 
        on_delete=models.CASCADE, 
        to_field='student_id', 
        db_column='student_id'
    )
    completed_aptitude = models.BooleanField()
    aptitude_score = models.FloatField()
    graduated = models.BooleanField()

class FullProfile(models.Model):
    student=models.ForeignKey(Student, on_delete=models.CASCADE)
    experience=models.ForeignKey(Experience, on_delete=models.CASCADE)
    referral=models.ForeignKey(Referral, on_delete=models.CASCADE)
    skill_level=models.ForeignKey(SkillLevel, on_delete=models.CASCADE)
    track=models.ForeignKey(Track, on_delete=models.CASCADE)
    hours_available=models.ForeignKey(HoursAvailable, on_delete=models.CASCADE)
    registration_date=models.ForeignKey(Registration, on_delete=models.CASCADE)
    outcomes=models.ForeignKey(Outcomes, on_delete=models.CASCADE)