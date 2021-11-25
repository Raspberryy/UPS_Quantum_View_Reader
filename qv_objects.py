from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.engine import URL
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, DateTime, select, ForeignKey, MetaData
import json
import hashlib
from datetime import datetime


# ************************************
# **      Connect to database       **
# ************************************
# ** Using SQLAlchemy to connect    **
# ** to SQL server and create DB    **
# ** model if needed.               **
# ************************************

connection_string = "DRIVER={ODBC Driver 17 for SQL Server};Server=;Database=;Trusted_Connection=Yes;"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = create_engine(connection_url)
base = declarative_base(metadata  = MetaData(schema="dbo"))

global_sessionmaker = sessionmaker(engine)
global_session = global_sessionmaker()

def createDBModel():
    base.metadata.create_all(engine)

# ************************************
# **       QV Classes/Tables        **
# ************************************
# ** In the following section all   **
# ** classes needed to store QV     **
# ** data are definied. Because OR  **
# ** mapping is used, the database  **
# ** structure is defined as well.  **   
# ** Columns are definied in        **
# ** accordance with UPS' QV XML    **
# ** documentation (Ver. June 23    **
# ** 2021)                          **                      
# ************************************

# *********************
# **    SHIPMENT     **
# ********************* 

class Shipment(base):
    __tablename__ = 'Shipment'

    ID = Column(Integer, primary_key=True)

    Filename = Column(String(16))

    manifest = relationship('Manifest')
    origin = relationship('Origin')
    exception = relationship('Exception')
    delivery = relationship('Delivery')
    generic = relationship('Generic')

    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())
    
# *********************
# **    MANIFEST     **
# ********************* 

class Manifest(base):
    __tablename__ = 'Manifest'

    ID = Column(Integer, primary_key=True)
    ShipmentID = Column(Integer, ForeignKey('Shipment.ID'), nullable=True)
    ShipperID = Column(Integer, ForeignKey('Shipper.ID'), nullable=False)
    ShipToID = Column(Integer, ForeignKey('ShipTo.ID'), nullable=False)

    referenceNumber = relationship('ReferenceNumber')
 
    ServiceCode = Column(String(3))
    ServiceDescription = Column(String(35))
    
    PickupDate = Column(String(8))
    ScheduledDeliveryDate = Column(String(8))
    ScheduledDeliveryTime = Column(String(6))
    DocumentsOnly = Column(String(1))

    package = relationship('Package')

    CallTagARSCode = Column(String(3))
    ManufactureCountry = Column(String(3))
    HarmonizedCode = Column(String(35))
    CustomsMonetaryValue = Column(String(16))
    SpecialInstructions = Column(String(35))
    ShipmentChargeType = Column(String(3))

    BillToAccountOption = Column(String(2))
    BillToAccountNumber = Column(String(10))

    LocationAssured = Column(String(1))
    ImportControl = Column(String(1))
    LabelDeliveryMethod = Column(String(3))
    CommercialInvoiceRemoval = Column(String(1))
    PostalServiceTrackingID = Column(String(35))
    ReturnsFlexibleAccess = Column(String(1))
    UPScarbonneutral = Column(String(1))
    #Product = Column(String())
    #UPSReturnsExchange = Column(String())
    #LiftGateOnDelivery = Column(String(1))
    #LiftGateOnPickUp = Column(String(1))
    #PickupPreference = Column(String(1))
    #DeliveryPreference = Column(String(1))
    #HoldForPickupAtUPSAccessPoint = Column(String(1))

    #UAPCompanyName = Column(String(35))
    #UAPAttentionName = Column(String(35))
    #UAPAddressLine1 = Column(String(35))
    #UAPAddressLine2 = Column(String(35))
    #UAPAddressLine3 = Column(String(35))
    #UAPCity = Column(String(30))
    #UAPStateProvinceCode = Column(String(5))
    #UAPPostalCode = Column(String(9))
    #UAPCountryCode = Column(String(2))
    #UAPPhoneNumber = Column(String(15))

    
    #DeliverToAddresseeOnlyIndicator = Column(String(1))
    #UPSAccessPointCODIndicator = Column(String(1))
    #ClinicalTrialIndicator = Column(String(1))
    #ClinicalTrialIndicationNumber = Column(String(20))
    #CategoryAHazardousIndicator = Column(String(1))
    #DirectDeliveryIndicator = Column(String(1))
    #PackageReleaseCodeIndicator = Column(String(1))
    #ProactiveResponseIndicator = Column(String(1))
    PackageCount = Column(String(9))
    #WhiteGloveDeliveryIndicator = Column(String(1))
    #RoomOfChoiceIndicator = Column(String(1))
    #InstallationDeliveryIndicator = Column(String(1))
    #ItemDisposalIndicator = Column(String(1))
    LeadShipmentTrackingNumber = Column(String(35))
    #SaturdayNonPremiumCommercialDeliveryIndicator = Column(String(1))
    #UPSPremierAccessorialIndicator = Column(String(1))
    #UPSPremierCategoryCode = Column(String(1))
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())


    
class Shipper(base):
    __tablename__ = 'Shipper'

    ID = Column(Integer, primary_key=True)        
    manifests = relationship('Manifest', backref="shipper")

    Name = Column(String(35))
    AttentionName = Column(String(35))
    #TaxIdentifiactionNumber = Column(String(15))
    PhoneNumber = Column(String(15))
    FaxNumber = Column(String(15))
    ShipperNumber = Column(String(6))
    EmailAddress = Column(String(50))
    AddressLine1 = Column(String(35))
    AddressLine2 = Column(String(35))
    AddressLine3 = Column(String(35))
    City = Column(String(30))
    StateProvinceCode = Column(String(5))
    PostalCode = Column(String(9))
    CountryCode = Column(String(2))
    
    Hash = Column(String(200), nullable=False, unique=True)
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())

    def __hash__(self):
        return self.Hash


class ShipTo(base):
    __tablename__ = 'ShipTo'

    ID = Column(Integer, primary_key=True)    
    manifests = relationship('Manifest', backref="shipTo")

    AssignedIdentificationNumber = Column(String(35))
    CompanyName = Column(String(35))
    AttentionName = Column(String(35))
    PhoneNumber = Column(String(15))
    #TaxIdentificationNumber = Column(String(15))
    FaxNumber = Column(String(15))

    ConsigneeName = Column(String(35))
    AddressLine1 = Column(String(35))
    AddressLine2 = Column(String(35))
    AddressLine3 = Column(String(35))
    City = Column(String(30))
    StateProvinceCode = Column(String(5))
    PostalCode = Column(String(9))
    CountryCode = Column(String(2))
    LocationID = Column(String(10))
    ReceivingAddressName = Column(String(35))

    Hash = Column(String(200), nullable=False, unique=True)
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())

    def __hash__(self):
        return self.Hash


class Package(base):
    __tablename__ = 'Package'

    ID = Column(Integer, primary_key=True)
    ManifestID = Column(Integer, ForeignKey('Manifest.ID'), nullable=False)
    
    packageActivity = relationship('PackageActivity')
    Description = Column(String(120))
    Length = Column(String(9))
    Width = Column(String(9))
    Height = Column(String(9))

    UOMCode = Column(String(3))
    UOMDescription = Column(String(35))
    Weight = Column(String(10))

    PackageWeight = Column(String(10))
    LargePackage = Column(String(1))
    TrackingNumber = Column(String(35))

    referenceNumber = relationship('ReferenceNumber')

    CODCode = Column(String(1))
    CODCurrencyCode = Column(String(3))
    CODMonetaryValue = Column(String(8))

    InsuredValueCurrencyCode = Column(String(1))
    InsuredValueMonetaryValue = Column(String(8))

    EarliestDeliveryTime = Column(String(6))
    HazardousMaterialsCode = Column(String(1))
    UPSPremiumCareIndicator = Column(String(1))
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())

    
class PackageActivity(base):
    __tablename__ = 'PackageActivity'

    ID = Column(Integer, primary_key=True)
    PackageID = Column(Integer, ForeignKey('Package.ID'), nullable=False)

    Date = Column(String(8))
    Time = Column(String(6))
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())

# ********************
# **    ORIGIN      **
# ********************
 
class Origin(base):
    __tablename__ = 'Origin'

    ID = Column(Integer, primary_key=True)
    ShipmentID = Column(Integer, ForeignKey('Shipment.ID'), nullable=False)

    referenceNumber = relationship('ReferenceNumber')
    
    ShipperNumber = Column(String(6))
    TrackingNumber = Column(String(18))
    Date = Column(String(8))
    Time = Column(String(6))
    PoliticalDivision2 = Column(String(30))
    PoliticalDivision1 = Column(String(5))
    CountryCode = Column(String(2))
    
    BillToAccountOption = Column(String(2))
    BillToAccountNumber = Column(String(10))

    ScheduledDeliveryDate = Column(String(8))
    ScheduledDeliveryTime = Column(String(6))
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())

# ********************
# **    EXCEPTION   **
# ********************

class Exception(base):
    __tablename__ = 'Exception'

    ID = Column(Integer, primary_key=True)
    ShipmentID = Column(Integer, ForeignKey('Shipment.ID'), nullable=False)

    referenceNumber = relationship('ReferenceNumber')
    
    ShipperNumber = Column(String(6))
    TrackingNumber = Column(String(18))
    Date = Column(String(8))
    Time = Column(String(6))

    ConsigneeName = Column(String(35))
    StreetNumberLow = Column(String(11))
    StreetPrefix = Column(String(2))
    StreetName = Column(String(50))
    StreetType = Column(String(10))
    StreetSuffix = Column(String(2))

    addressExtendedInformation = relationship('AddressExtendedInformation')

    PoliticalDivision3 = Column(String(30))
    PoliticalDivision2 = Column(String(30))
    PoliticalDivision1 = Column(String(5))
    CountryCode = Column(String(3))
    PostcodePrimaryLow = Column(String(10))
    
    StatusCode = Column(String(2))
    StatusDescription = Column(String(120))
    ReasonCode = Column(String(2))
    ReasonDescription = Column(String(120))
    ResolutionCode = Column(String(2))
    ResolutionDescription = Column(String(120))

    RescheduledDeliveryDate = Column(String(8))
    RescheduledDeliveryTime = Column(String(6))

    PoliticalDivision2 = Column(String(30))
    PoliticalDivision1 = Column(String(5))
    CountryCode = Column(String(2))

    BillToAccountOption = Column(String(2))
    BillToAccountNumber = Column(String(250))

    AccessPointLocationID = Column(String(9))
    SimplifiedText = Column(String(240))
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())


class AddressExtendedInformation(base):
    __tablename__ = 'AddressExtendedInformation'

    ID = Column(Integer, primary_key=True)
    ExceptionID = Column(Integer, ForeignKey('Exception.ID'), nullable=True)
    DeliveryID = Column(Integer, ForeignKey('Delivery.ID'), nullable=True)

    Type = Column(String(40))
    Low = Column(String(11))
    High = Column(String(3))
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())

# ********************
# **    Delivery    **
# ********************

class Delivery(base):
    __tablename__ = 'Delivery'

    ID = Column(Integer, primary_key=True)
    ShipmentID = Column(Integer, ForeignKey('Shipment.ID'), nullable=False)

    referenceNumber = relationship('ReferenceNumber')
    
    ShipperNumber = Column(String(6))
    TrackingNumber = Column(String(18))
    Date = Column(String(8))
    Time = Column(String(6))

    DriverRelease = Column(String(40))
    PoliticalDivision2 = Column(String(30))
    PoliticalDivision1 = Column(String(5))
    CountryCode = Column(String(2))

    ConsigneeName = Column(String(35))
    StreetNumberLow = Column(String(11))
    StreetPrefix = Column(String(2))
    StreetName = Column(String(50))
    StreetType = Column(String(10))
    StreetSuffix = Column(String(2))
    BuildingName = Column(String(40))
    
    addressExtendedInformation = relationship('AddressExtendedInformation')

    PoliticalDivision3 = Column(String(30))
    PoliticalDivision2 = Column(String(30))
    PoliticalDivision1 = Column(String(5))
    CountryCode = Column(String(3))
    PostcodePrimaryLow = Column(String(15))
    PostcodeExtendedLow = Column(String(4))

    DeliveryLocationCode = Column(String(16))
    DeliveryLocationDescription = Column(String(15))
    
    SignedForByName = Column(String(35))

    CODCurrencyCode = Column(String(3))
    CODMonetaryValue = Column(String(8))

    BillToAccountOption = Column(String(2))
    BillToAccountNumber = Column(String(10))
    
    LastPickupDate = Column(String(10))
    AccessPointLocationID = Column(String(9))
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())

# ********************
# **    GENERIC     **
# ******************** 

class Generic(base):
    __tablename__ = 'Generic'

    ID = Column(Integer, primary_key=True)
    ShipmentID = Column(Integer, ForeignKey('Shipment.ID'), nullable=False)

    ActivityType = Column(String(15))
    TrackingNumber = Column(String(35))
    ShipperNumber = Column(String(10))

    referenceNumber = relationship('ReferenceNumber')
    
    ServiceCode = Column(String(3))
    ServiceDescription = Column(String(250))

    ActivityDate = Column(String(8))
    ActivityTime = Column(String(6))

    BillToAccountOption = Column(String(2))
    BillToAccountNumber = Column(String(10))

    ShipToLocationID = Column(String(10))
    ShipToReceivingAddressName = Column(String(35))
    ShipToBookmark = Column(String(1))
    
    RescheduledDeliveryDate = Column(String(8))

    FailedEmailAddress = Column(String(50))
    FailureNotificationCode = Column(String(2))
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())

    


class ReferenceNumber(base):
    __tablename__ = 'ReferenceNumber'

    ID = Column(Integer, primary_key=True)
    GenericID = Column(Integer, ForeignKey('Generic.ID'), nullable=True)
    ManifestID = Column(Integer, ForeignKey('Manifest.ID'), nullable=True)
    OriginID = Column(Integer, ForeignKey('Origin.ID'), nullable=True)
    DeliveryID = Column(Integer, ForeignKey('Delivery.ID'), nullable=True)
    ExceptionID = Column(Integer, ForeignKey('Exception.ID'), nullable=True)
    PackageID = Column(Integer, ForeignKey('Package.ID'), nullable=True)

    Number = Column(String(1))
    Code = Column(String(3))
    Value = Column(String(35))

    ReferenceNumberType = Column(String(8))
    CreatedOn = Column(DateTime(timezone=True), server_default=func.now())


# ************************************
# **    QV JSON Repsonse Parser     **
# ************************************
# ** The following section contains **
# ** methods for parsing JSON QV    **
# ** responses.                     **
# ** In addition the helper funcs   **
# ** getValue() and getNodeAsList() **
# ** are used to check and get      **
# ** Node values of response        **
# ************************************

# ** Caching Objects from DB for faster Parsing **
shipper_cache = {}
shipTo_cache = {}


def getValue(keys, node):
    """Iterates thorugh keys along node:
    node[keys[0]][keys[1]]...
    Checks if keys[n] exists in node.
    Returns None if key does not exist.
    """

    value = None

    while len(keys) > 0:
        key = keys.pop(0)

        if key in node:
            if len(keys) == 0:
                value = node[key]
            else:
                node = node[key]
        else:
            break

    return value



def getNodeAsList(key, node):
    """Checks if key is in node and returns key as list. 
    Creates new list if is single element.
    Returns empty list on error.
    """

    nodeAsList = []

    if node is None or key in node:
        listNode = node[key]
        if isinstance(listNode, list):
            nodeAsList = listNode
        else:
            nodeAsList.append(listNode)

    return nodeAsList


def getShipmentfromNode(node):

    shipment = Shipment()
    shipment.Filename = getValue(["FileName"], node)
    return shipment


def mergeShipper(_shipper):

    # Check if shipper is cached
    global shipper_cache
    cached_Shipper = shipper_cache.get(_shipper.Hash)

    if cached_Shipper is None:
        shipper_cache[_shipper.Hash] = _shipper
    else:
        _shipper = cached_Shipper

    return _shipper


def getShipperfromNode(node):
    shipper = Shipper()

    shipper.Name = getValue(["Name"], node)
    shipper.AttentionName = getValue(["AttentionName"], node)
    shipper.PhoneNumber = getValue(["PhoneNumber"], node)
    shipper.FaxNumber = getValue(["FaxNumber"], node)
    shipper.ShipperNumber = getValue(["ShipperNumber"], node)
    shipper.EMailAddress = getValue(["EMailAddress"], node)
    shipper.AddressLine1 = getValue(["Address", "AddressLine1"], node)
    shipper.AddressLine2 = getValue(["Address", "AddressLine2"], node)
    shipper.AddressLine3 = getValue(["Address", "AddressLine3"], node)
    shipper.City = getValue(["Address", "City"], node)
    shipper.StateProvinceCode = getValue(["Address", "StateProvinceCode"], node)
    shipper.PostalCode = getValue(["Address", "PostalCode"], node)
    shipper.CountryCode = getValue(["Address", "CountryCode"], node)

    # Check if object exists in DB
    shipper.Hash = hashlib.md5(json.dumps(node).encode('utf-8')).hexdigest()
    return mergeShipper(shipper)


def mergeShipTo(_shipTo):

    # Check if shipTo is cached
    global shipTo_cache
    cached_shipTo = shipTo_cache.get(_shipTo.Hash)

    if cached_shipTo is None:
        shipTo_cache[_shipTo.Hash] = _shipTo
    else:
        _shipTo = cached_shipTo

    return _shipTo

def getShipTofromNode(node):
    shipTo = ShipTo()
    
    shipTo.AssignedIdentificationNumber = getValue(["ShipperAssignedIdentificationNumber"], node)
    shipTo.CompanyName = getValue(["CompanyName"], node)
    shipTo.AttentionName = getValue(["AttentionName"], node)
    shipTo.PhoneNumber = getValue(["PhoneNumber"], node)
    shipTo.FaxNumber = getValue(["FaxNumber"], node)
    shipTo.ConsigneeName = getValue(["Address", "ConsigneeName"], node)
    shipTo.AddressLine1 = getValue(["Address", "AddressLine1"], node)
    shipTo.AddressLine2 = getValue(["Address", "AddressLine2"], node)
    shipTo.AddressLine3 = getValue(["Address", "AddressLine3"], node)
    shipTo.City = getValue(["Address", "City"], node)
    shipTo.StateProvinceCode = getValue(["Address", "StateProvinceCode"], node)
    shipTo.PostalCode = getValue(["Address", "PostalCode"], node)
    shipTo.CountryCode = getValue(["Address", "CountryCode"], node)
    shipTo.LocationID = getValue(["LocationID"], node)
    shipTo.ReceivingAddressName = getValue(["ReceivingAddressName"], node)
    
    # Check if object exists in DB
    shipTo.Hash = hashlib.md5(json.dumps(node).encode('utf-8')).hexdigest()
    return mergeShipTo(shipTo)

def getShipmentReferenceNumberfromNode(node):
    shipmentReferenceNumber = ReferenceNumber()
    shipmentReferenceNumber.ReferenceNumberType="Shipment"

    shipmentReferenceNumber.Number = getValue(["Number"], node)
    shipmentReferenceNumber.Code = getValue(["Code"], node)
    shipmentReferenceNumber.Value = getValue(["Value"], node)

    return shipmentReferenceNumber

def getPackageActivityfromNode(node):
    packageActivity = PackageActivity()

    packageActivity.Date = getValue(["Date"], node)
    packageActivity.Time = getValue(["Time"], node)

    return packageActivity

def getPackageReferenceNumberfromNode(node):
    packageReferenceNumber = ReferenceNumber()
    packageReferenceNumber.ReferenceNumberType = "Package"

    packageReferenceNumber.Number = getValue(["Number"], node)
    packageReferenceNumber.Code = getValue(["Code"], node)
    packageReferenceNumber.Value = getValue(["Value"], node)

    return packageReferenceNumber

def getPackagefromNode(node):
    package = Package()

    for packageActivityNode in getNodeAsList("Activity", node):
        package.packageActivity.append(getPackageActivityfromNode(packageActivityNode))
        
    package.Description = getValue(["Description"], node)
    package.Length = getValue(["Dimensions", "Length"], node)
    package.Width = getValue(["Dimensions", "Width"], node)
    package.Height = getValue(["Dimensions", "Height"], node)

    package.UOMCode = getValue(["DimensionalWeight", "UnitOfMeasurement", "Code"], node)
    package.UOMDescription = getValue(["DimensionalWeight", "UnitOfMeasurement", "Description"], node)
    package.Weight = getValue(["DimensionalWeight", "Weight"], node)

    package.PackageWeight = getValue(["PackageWeight", "Weight"], node)
    package.LargePackage = getValue(["LargePackage"], node)
    package.TrackingNumber = getValue(["TrackingNumber"], node)

    for packageReferenceNumberNode in getNodeAsList("ReferenceNumber", node):
        package.referenceNumber.append(getPackageReferenceNumberfromNode(packageReferenceNumberNode))

    package.CODCode = getValue(["PackageServiceOptions", "COD", "CODCode"], node)
    package.CODCurrencyCode = getValue(["PackageServiceOptions", "COD", "CODAmount", "CurrencyCode"], node)
    package.CODMonetaryValue = getValue(["PackageServiceOptions", "COD", "CODAmount", "MonetaryValue"], node)

    package.InsuredValueCurrencyCode = getValue(["PackageServiceOptions", "InsuredValue", "CurrencyCode"], node)
    package.InsuredValueMonetaryValue = getValue(["PackageServiceOptions", "InsuredValue", "MonetaryValue"], node)

    package.EarliestDeliveryTime = getValue(["PackageServiceOptions", "EarliestDeliveryTime"], node)
    package.HazardousMaterialsCode = getValue(["PackageServiceOptions", "HazardousMaterialsCode"], node)
    package.UPSPremiumCareIndicator = getValue(["UPSPremiumCareIndicator"], node)

    return package

def getManifestfromNode(node):

    manifest = Manifest()

    if "Shipper" in node:
        manifest.shipper = getShipperfromNode(node["Shipper"])

    if "ShipTo" in node:
        manifest.shipTo = getShipTofromNode(node["ShipTo"])
                
    for referenceNumberNode in getNodeAsList("ReferenceNumber", node):
        manifest.referenceNumber.append(getShipmentReferenceNumberfromNode(referenceNumberNode))

    manifest.ServiceCode = getValue(["Service", "Code"], node)
    manifest.ServiceDescription = getValue(["Service", "Description"], node)
    manifest.PickupDate = getValue(["PickupDate"], node)
    manifest.ScheduledDeliveryDate = getValue(["ScheduledDeliveryDate"], node)
    manifest.ScheduledDeliveryTime = getValue(["ScheduledDeliveryTime"], node)
    manifest.DocumentsOnly = getValue(["DocumentsOnly"], node)

    for packageNode in getNodeAsList("Package", node):
        manifest.package.append(getPackagefromNode(packageNode))

    manifest.CallTagARSCode = getValue(["ShipmentServiceOptions", "CallTagARS", "Code"], node)
    manifest.ManufactureCountry = getValue(["ManufactureCountry"], node)
    manifest.HarmonizedCode = getValue(["HarmonizedCode"], node)
    manifest.CustomsMonetaryValue = getValue(["CustomsValue", "MonetaryValue"], node)
    manifest.SpecialInstructions = getValue(["SpecialInstructions"], node)
    manifest.ShipmentChargeType = getValue(["ShipmentChargeType"], node)
    manifest.BillToAccountOption = getValue(["BillToAccount", "Option"], node)
    manifest.BillToAccountNumber = getValue(["BillToAccount", "Number"], node)
    manifest.LocationAssured = getValue(["LocationAssured"], node)
    manifest.ImportControl = getValue(["ImportControl"], node)
    manifest.LabelDeliveryMethod = getValue(["LabelDeliveryMethod"], node)
    manifest.CommercialInvoiceRemoval = getValue(["CommercialInvoiceRemoval"], node)
    manifest.PostalServiceTrackingID = getValue(["PostalServiceTrackingID"], node)
    manifest.ReturnsFlexibleAccess = getValue(["ReturnsFlexibleAccess"], node)
    manifest.UPScarbonneutral = getValue(["UPScarbonneutral"], node)
    manifest.PackageCount = getValue(["PackageCount"], node)
    manifest.LeadShipmentTrackingNumber = getValue(["LeadShipmentTrackingNumber"], node)

    return manifest


def getOriginfromNode(node):

    origin = Origin()
    
    for shipmentReferenceNumberNode in getNodeAsList("ShipmentReferenceNumber", node):
        origin.referenceNumber.append(getShipmentReferenceNumberfromNode(shipmentReferenceNumberNode))

    for packageReferenceNumberNode in getNodeAsList("PackageReferenceNumber", node):
        origin.referenceNumber.append(getPackageReferenceNumberfromNode(packageReferenceNumberNode))

    origin.ShipperNumber = getValue(["ShipperNumber"], node)
    origin.TrackingNumber = getValue(["TrackingNumber"], node)
    origin.Date = getValue(["Date"], node)
    origin.Time = getValue(["Time"], node)
    origin.PoliticalDivision2 = getValue(["ActivityLocation", "AddressArtifactFormat", "PoliticalDivision2"], node)
    origin.PoliticalDivision1 = getValue(["ActivityLocation", "AddressArtifactFormat", "PoliticalDivision1"], node)
    origin.CountryCode = getValue(["ActivityLocation", "AddressArtifactFormat", "CountryCode"], node)
    origin.BillToAccountOption = getValue(["BillToAccount", "Option"], node)
    origin.BillToAccountNumber = getValue(["BillToAccount", "Number"], node)
    origin.ScheduledDeliveryDate = getValue(["ScheduledDeliveryDate"], node)
    origin.ScheduledDeliveryTime = getValue(["ScheduledDeliveryTime"], node)

    return origin

def getAddressExtendedInformationfromNode(node):

    addressExtendedInformation = AddressExtendedInformation()
        
    addressExtendedInformation.Type = getValue(["Type"], node)
    addressExtendedInformation.Low = getValue(["Low"], node)
    addressExtendedInformation.High = getValue(["High"], node)

    return addressExtendedInformation

def getExceptionfromNode(node):

    exception = Exception()

    for shipmentReferenceNumberNode in getNodeAsList("ShipmentReferenceNumber", node):
        exception.referenceNumber.append(getShipmentReferenceNumberfromNode(shipmentReferenceNumberNode))

    for packageReferenceNumberNode in getNodeAsList("PackageReferenceNumber", node):
        exception.referenceNumber.append(getPackageReferenceNumberfromNode(packageReferenceNumberNode))

    exception.ShipperNumber = getValue(["ShipperNumber"], node)
    exception.TrackingNumber = getValue(["TrackingNumber"], node)
    exception.Date = getValue(["Date"], node)
    exception.Time = getValue(["Time"], node)
    exception.ConsigneeName = getValue(["UpdatedAddress", "ConsigneeName"], node)
    exception.StreetNumberLow = getValue(["UpdatedAddress", "StreetNumberLow"], node)
    exception.StreetPrefix = getValue(["UpdatedAddress", "StreetPrefix"], node)
    exception.StreetName = getValue(["UpdatedAddress", "StreetName"], node)
    exception.StreetType = getValue(["UpdatedAddress", "StreetType"], node)
    exception.StreetSuffix = getValue(["UpdatedAddress", "StreetSuffix"], node)

    if "UpdatedAddress" in node:
        for addressExtendedInformationNode in getNodeAsList("AddressExtendedInformation", node["UpdatedAddress"]):
            exception.addressExtendedInformation.append(getAddressExtendedInformationfromNode(addressExtendedInformationNode))

    exception.PoliticalDivision3 = getValue(["UpdatedAddress", "PoliticalDivision3"], node)
    exception.PoliticalDivision2 = getValue(["UpdatedAddress", "PoliticalDivision2"], node)
    exception.PoliticalDivision1 = getValue(["UpdatedAddress", "PoliticalDivision1"], node)
    exception.CountryCode = getValue(["UpdatedAddress", "CountryCode"], node)
    exception.PostcodePrimaryLow = getValue(["UpdatedAddress", "PostcodePrimaryLow"], node)
    exception.StatusCode = getValue(["StatusCode"], node)
    exception.StatusDescription = getValue(["StatusDescription"], node)
    exception.ReasonCode = getValue(["ReasonCode"], node)
    exception.ReasonDescription = getValue(["ReasonDescription"], node)
    exception.ResolutionCode = getValue(["Resolution", "Code"], node)
    exception.ResolutionDescription = getValue(["Resolution", "Description"], node)
    exception.RescheduledDeliveryDate = getValue(["RescheduledDeliveryDate"], node)
    exception.RescheduledDeliveryTime = getValue(["RescheduledDeliveryTime"], node)
    exception.PoliticalDivision2 = getValue(["ActivityLocation", "AddressArtifactFormat", "PoliticalDivision2"], node)
    exception.PoliticalDivision1 = getValue(["ActivityLocation", "AddressArtifactFormat", "PoliticalDivision1"], node)
    exception.CountryCode = getValue(["ActivityLocation", "AddressArtifactFormat", "CountryCode"], node)
    exception.BillToAccountOption = getValue(["BillToAccount", "Option"], node)
    exception.BillToAccountNumber = getValue(["BillToAccount", "Number"], node)
    exception.AccessPointLocationID = getValue(["AccessPointLocationID"], node)
    exception.SimplifiedText = getValue(["SimplifiedText"], node)

    return exception



def getDeliveryfromNode(node):

    delivery = Delivery()

    for shipmentReferenceNumberNode in getNodeAsList("ShipmentReferenceNumber", node):
        delivery.referenceNumber.append(getShipmentReferenceNumberfromNode(shipmentReferenceNumberNode))

    for packageReferenceNumberNode in getNodeAsList("PackageReferenceNumber", node):
        delivery.referenceNumber.append(getPackageReferenceNumberfromNode(packageReferenceNumberNode))

    delivery.ShipperNumber = getValue(["ShipperNumber"], node)
    delivery.TrackingNumber = getValue(["TrackingNumber"], node)
    delivery.Date = getValue(["Date"], node)
    delivery.Time = getValue(["Time"], node)
    delivery.DriverRelease = getValue(["DriverRelease"], node)
    delivery.PoliticalDivision2 = getValue(["ActivityLocation", "AddressArtifactFormat", "PoliticalDivision2"], node)
    delivery.PoliticalDivision1 = getValue(["ActivityLocation", "AddressArtifactFormat", "PoliticalDivision1"], node)
    delivery.CountryCode = getValue(["ActivityLocation", "AddressArtifactFormat", "CountryCode"], node)
    delivery.ConsigneeName = getValue(["DeliveryLocation", "AddressArtifactFormat", "ConsigneeName"], node)
    delivery.StreetNumberLow = getValue(["DeliveryLocation", "AddressArtifactFormat", "StreetNumberLow"], node)
    delivery.StreetPrefix = getValue(["DeliveryLocation", "AddressArtifactFormat", "StreetPrefix"], node)
    delivery.StreetName = getValue(["DeliveryLocation", "AddressArtifactFormat", "StreetName"], node)
    delivery.StreetType = getValue(["DeliveryLocation", "AddressArtifactFormat", "StreetType"], node)
    delivery.StreetSuffix = getValue(["DeliveryLocation", "AddressArtifactFormat", "StreetSuffix"], node)
    delivery.BuildingName = getValue(["DeliveryLocation", "AddressArtifactFormat", "BuildingName"], node)

    if "DeliveryLocation" in node:
        if "AddressArtifactFormat" in node["DeliveryLocation"]:
            for addressExtendedInformationNode in getNodeAsList("AddressExtendedInformation", node["DeliveryLocation"]["AddressArtifactFormat"]):
                delivery.addressExtendedInformation.append(getAddressExtendedInformationfromNode(addressExtendedInformationNode))

    delivery.PoliticalDivision3 = getValue(["DeliveryLocation", "AddressArtifactFormat", "PoliticalDivision3"], node)
    delivery.PoliticalDivision2 = getValue(["DeliveryLocation", "AddressArtifactFormat", "PoliticalDivision2"], node)
    delivery.PoliticalDivision1 = getValue(["DeliveryLocation", "AddressArtifactFormat", "PoliticalDivision1"], node)
    delivery.CountryCode = getValue(["DeliveryLocation", "AddressArtifactFormat", "CountryCode"], node)
    delivery.PostcodePrimaryLow = getValue(["DeliveryLocation", "AddressArtifactFormat", "PostcodePrimaryLow"], node)
    delivery.PostcodeExtendedLow = getValue(["DeliveryLocation", "AddressArtifactFormat", "PostcodeExtendedLow"], node)
    delivery.DeliveryLocationCode = getValue(["DeliveryLocation", "AddressArtifactFormat", "ResidentialAddressIndicator", "Code"], node)
    delivery.DeliveryLocationDescription = getValue(["DeliveryLocation", "AddressArtifactFormat", "ResidentialAddressIndicator", "Description"], node)
    delivery.SignedForByName = getValue(["DeliveryLocation", "SignedForByName"], node)
    delivery.CODCurrencyCode = getValue(["COD", "CODAmount", "CurrencyCode"], node)
    delivery.CODMonetaryValue = getValue(["COD", "CODAmount", "MonetaryValue"], node)
    delivery.BillToAccountOption = getValue(["BillToAccount", "Option"], node)
    delivery.BillToAccountNumber = getValue(["BillToAccount", "Number"], node)
    delivery.LastPickupDate = getValue(["LastPickupDate"], node)
    delivery.AccessPointLocationID = getValue(["AccessPointLocationID"], node)

    return delivery

def getGenericfromNode(node):

    generic = Generic()
    
    generic.ActivityType = getValue(["ActivityType"], node)
    generic.TrackingNumber = getValue(["TrackingNumber"], node)
    generic.ShipperNumber = getValue(["ShipperNumber"], node)
    
    for shipmentReferenceNumberNode in getNodeAsList("ShipmentReferenceNumber", node):
        generic.referenceNumber.append(getShipmentReferenceNumberfromNode(shipmentReferenceNumberNode))

    for packageReferenceNumberNode in getNodeAsList("PackageReferenceNumber", node):
        generic.referenceNumber.append(getPackageReferenceNumberfromNode(packageReferenceNumberNode))

    generic.ServiceCode = getValue(["Service", "Code"], node)
    generic.ServiceDescription = getValue(["Service", "ServiceDescription"], node)
    
    generic.ActivityDate = getValue(["Activity", "Date"], node)
    generic.ActivityTime = getValue(["Activity", "Time"], node)

    generic.BillToAccountOption = getValue(["BillToAccount", "Option"], node)
    generic.BillToAccountNumber = getValue(["BillToAccount", "Number"], node)

    generic.ShipToLocationID = getValue(["ShipTo", "LocationID"], node)
    generic.ShipToReceivingAddressName = getValue(["ShipTo", "ReceivingAddressName"], node)
    generic.ShipToBookmark = getValue(["ShipTo", "Bookmark"], node)
   
    generic.RescheduledDeliveryDate = getValue(["RescheduledDeliveryDate"], node)

    generic.FailedEmailAddress = getValue(["FailureNotification", "FailedEmailAddress"], node)
    generic.FailureNotificationCode = getValue(["FailureNotification", "FailureNotificationCode", "Code"], node)

    return generic


def loadCache():
    global shipper_cache
    global shipTo_cache

    print("\t" + str(datetime.now()) + "\tLoading files to cache")

    if len(shipper_cache) == 0:
        shipper_list = global_session.query(Shipper).all()
        shipper_cache = dict((shipper.Hash, shipper) for shipper in shipper_list)

    if len(shipTo_cache) == 0:
        shipTo_list = global_session.query(ShipTo).all()
        shipTo_cache = dict((shipTo.Hash, shipTo) for shipTo in shipTo_list)





def parseShipments(jsonObj):

    # Check if response contains any data
    subscriptionFileNode = getNodeAsList(
            'SubscriptionFile', 
            getValue(['QuantumViewResponse','QuantumViewEvents','SubscriptionEvents'], jsonObj))
    if subscriptionFileNode is None or len(subscriptionFileNode) == 0:
        print("\t" + str(datetime.now()) + "\t[ERROR] No data found")
        return []
    

    # Load Cache
    loadCache()

    # Start parsing
    print("\t" + str(datetime.now()) + "\tStart parsing data")
    shipments = []
    totalShipments = len(subscriptionFileNode)


    for shipmentNode in subscriptionFileNode:

        shipment = getShipmentfromNode(shipmentNode)

        for manifestNode in getNodeAsList("Manifest", shipmentNode):
                shipment.manifest.append(getManifestfromNode(manifestNode))

        for originNode in getNodeAsList("Origin", shipmentNode):
            shipment.origin.append(getOriginfromNode(originNode))

        for exceptionNode in getNodeAsList("Exception", shipmentNode):
            shipment.exception.append(getExceptionfromNode(exceptionNode))

        for deliveryNode in getNodeAsList("Delivery", shipmentNode):
            shipment.delivery.append(getDeliveryfromNode(deliveryNode))

        for genericNode in getNodeAsList("Generic", shipmentNode):
            shipment.generic.append(getGenericfromNode(genericNode))

        shipments.append(shipment)
    
    return shipments

'''
DROP TABLE ReferenceNumber;
DROP TABLE PackageActivity;
DROP TABLE AddressExtendedInformation;
DROP TABLE Package;
DROP TABLE Generic;
DROP TABLE Manifest;
DROP TABLE Shipper;
DROP TABLE ShipTo;
DROP TABLE Origin;
DROP TABLE Exception;
DROP TABLE Delivery;
DROP TABLE Shipment;
'''