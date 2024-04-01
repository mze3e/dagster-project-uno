"""
resources
"""
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from random import Random
from typing import Sequence, Union

from dagster import ConfigurableResource
from faker import Faker
from pydantic import Field

# This file holds a resource you'll use in the tutorial
# You won't need to use this file/class until the
# Connecting to External Services section of the tutorial (Part 8).
# Once you are on Part 8, you will the contents of this file, but you
# don't need to understand the underlying code.


# To the curious user: This is the underlying code to generate the signups
@dataclass
class Signup:
    # pylint: disable=too-many-instance-attributes

    """
    Represents a signup object with various properties.

    Attributes:
        name (str): The name of the signup.
        email (str): The email of the signup.
        country (str): The country of the signup.
        signup_source (str): The source of the signup.
        referral (str): The referral of the signup.
        signup_purpose (str): The purpose of the signup.
        subscription_level (str): The level of subscription.
        payment_method (str): The payment method used for signup.
        sso_id (str): The single sign-on ID of the signup.
        email_verified (bool): Indicates if the email is verified.
        enabled (bool): Indicates if the signup is enabled.
        registered_at (datetime): The datetime when the signup was registered.
    """

    name: str
    email: str
    country: str
    signup_source: str
    referral: str
    signup_purpose: str
    subscription_level: str
    payment_method: str
    sso_id: str
    email_verified: bool
    enabled: bool
    registered_at: datetime

    def to_dict(self) -> dict:
        """
        Converts the object to a dictionary representation.

        Returns:
            dict: A dictionary containing the object's properties.
        """
        props = {k: v for k, v in asdict(self).items() if not k.startswith("_")}
        props["registered_at"] = self.registered_at.isoformat()
        return props

    def properties(self):
        """
        Returns a tuple containing the properties of the object.

        Returns:
            tuple: A tuple containing the following properties:
                - name (str): The name of the object.
                - email (str): The email of the object.
                - country (str): The country of the object.
                - signup_source (str): The signup source of the object.
                - referral (str): The referral of the object.
                - signup_purpose (str): The signup purpose of the object.
                - subscription_level (str): The subscription level of the object.
                - payment_method (str): The payment method of the object.
                - sso_id (str): The SSO ID of the object.
                - email_verified (bool): Indicates if the email is verified or not.
                - enabled (bool): Indicates if the object is enabled or not.
                - registered_at (datetime): The registration timestamp of the object.
        """
        return (
            self.name,
            self.email,
            self.country,
            self.signup_source,
            self.referral,
            self.signup_purpose,
            self.subscription_level,
            self.payment_method,
            self.sso_id,
            self.email_verified,
            self.enabled,
            self.registered_at,
        )

    def __eq__(self, other):
        if type(other) is type(self):
            return self.properties() == other.properties()
        return False

    def __hash__(self):
        return hash(self.properties())

    def __getitem__(self, key):
        return getattr(self, key)


class DataGenerator:
    """
    A class that generates fake signups data.

    Args:
        seed (int): The seed value for random number generation. Defaults to 0.

    Attributes:
        seed (int): The seed value for random number generation.
        fake (Faker): An instance of the Faker class for generating fake data.
        random (Random): An instance of the Random class for generating random numbers.
    """

    def __init__(self, seed: int = 0):
        self.seed = seed
        self.fake = Faker()
        self.random = Random(seed)

    def generate_signup(self, date) -> Signup:
        """
        Generates a fake signup object for the given date.

        Args:
            date (datetime): The date for which the signup is generated.

        Returns:
            Signup: The generated signup object.
        """
        registered_at = self.fake.date_time_between_dates(date, date + timedelta(days=1))

        return Signup(
            name=self.fake.name(),
            email=self.fake.email(),
            country=self.fake.country(),
            signup_source=self.fake.random_element(["google", "facebook", "twitter", "other"]),
            referral=self.fake.uri(),
            signup_purpose=self.fake.random_element(["personal", "business", "education", "other"]),
            subscription_level=self.fake.random_element(["trial", "free", "premium", "enterprise"]),
            payment_method=self.fake.random_element(["credit_card", "paypal", "check", "other"]),
            sso_id=self.fake.uuid4(),
            email_verified=self.fake.boolean(),
            enabled=self.fake.boolean(),
            registered_at=registered_at,
        )

    def get_signups_for_date(self, date: datetime) -> Sequence[Signup]:
        """
        Generates a list of fake signups for the given date.

        Args:
            date (datetime): The date for which the signups are generated.

        Returns:
            Sequence[Signup]: The list of generated signups.
        """
        date_to_seed = date.strftime("%Y%m%d")
        Faker.seed(date_to_seed)
        self.random = Random(date_to_seed)

        signups = []
        num_signups = self.random.randint(25, 100)

        for _ in range(num_signups):
            signup = self.generate_signup(date)
            signups.append(signup.to_dict())

        new_seed = self.random.randint(0, 100000)
        Faker.seed(new_seed)
        self.random = Random(new_seed)
        return sorted(signups, key=lambda x: x["registered_at"])

    def get_signups_for_dates(
        self, start_date: datetime, end_date: Union[datetime, None] = None
    ) -> Sequence[Signup]:
        """
        Generates a list of fake signups for a range of dates.

        Args:
            start_date (datetime): The start date of the range.
            end_date (datetime, optional): The end date of the range. Defaults to None.

        Returns:
            Sequence[Signup]: The list of generated signups.
        """
        signups = []

        end_date_to_use = end_date or (datetime.now() - timedelta(days=1))
        current_date = start_date

        while current_date < end_date_to_use:
            signups.extend(self.get_signups_for_date(current_date))
            current_date += timedelta(days=1)

        return signups

    def get_signups(self, num_days: int = 7) -> Sequence[Signup]:
        """
        Generates a list of fake signups for the specified number of days.

        Args:
            num_days (int): The number of days for which signups are generated. Defaults to 7.

        Returns:
            Sequence[Signup]: The list of generated signups.
        """
        start_date = datetime.now() - timedelta(days=num_days)

        return self.get_signups_for_dates(start_date)


class DataGeneratorResource(ConfigurableResource):
    """Resource for generating simulated data for experimenting with Dagster.

    Examples:
        .. code-block:: python
            from dagster import Definitions, asset
            from dagster_data_generator import DataGeneratorResource, DataGeneratorConfig

            @asset
            def my_table(data_gen: DataGeneratorConfig):
                return data_gen.get_signups()

            defs = Definitions(
                assets=[my_table],
                resources={"data_gen": DataGeneratorResource()}
            )
    """

    seed: int = Field(
        description=(
            "Seed for the random number generator. If not provided, a static seed will be used."
        ),
        default=0,
    )

    num_days: int = Field(
        description="Number of days to generate data for. Defaults to 7", default=7
    )

    @property
    def generator(self) -> DataGenerator:
        """
        Returns a DataGenerator object initialized with the seed value.

        Returns:
            DataGenerator: The initialized DataGenerator object.
        """
        return DataGenerator(self.seed)

    def get_signups(self):
        """
        Retrieves signups for a specified number of days.

        Returns:
            list: A list of signups.
        """
        result = []
        today = datetime.now()

        for i in range(self.num_days):
            yday = today - timedelta(days=i)
            result.extend(self.generator.get_signups_for_date(yday))

        return result

    def get_signups_for_date(self, date: str):
        """
        Retrieves signups for a given date.

        Args:
            date (str): The date in the format "mm-dd-yyyy".

        Returns:
            List: A list of signups for the specified date.
        """
        date_obj = datetime.strptime(date, "%m-%d-%Y")
        return self.generator.get_signups_for_date(date_obj)
