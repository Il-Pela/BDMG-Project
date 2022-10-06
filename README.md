# Italian's Electricity & Gas Bill - Big Data Management project
In this project we Prepared and Analyzed a dataset about Italian's bills. <br />
The dataset contained about 14 Millions of rows and every group choosed a library in a give list in order to do the work. <br />We selected [Rapids](https://rapids.ai/) (cuDf)
but, due to the dataset dimensions, we decided to change and use Dask-cuDf, a distributed version of Rapids. <br />
For more info: [project's slides]()
### Contributors 
[Francesco Pelacani](https://github.com/Il-Pela) <br />
[Giulio Querzoli](https://github.com/Giurzo) <br />
[Mirco Botti](https://github.com/JCobot) <br />
## Pipeline
<img src="/img/first_step.jpg" alt="Employee data" title="Pipeline 1">
<img src="/img/second_step.jpg" alt="Employee data" title="Pipeline 1">


## Data's Schema

| Col name | Target data type | Description |
| --- | --- | --- |
| **user_code** | String | (Anonymized) code for the customer that owns this utility |
| **customer_code** | String | Combined with user_code provides a unique identifier for the utility. Even this field is anonymized |
| **city** | String | City where the utility is located |
| **address** | String | (Anonymized) address of the utility location |
| **user_code** | String | (Anonymized) code that identifies the customer |
| **nominative** | String | (Anonymized) customer name |
| **sex** | String | Sex of the customer. It could be ‘M’, ‘F’, ‘P’, with ‘P’ denoting that the customer is a commercial activity (VAT number) |
| **age** | Int | Age of the customer, set to null for commercial activities (sex = ‘P’). Its value must be >= 18 |
| **bill_id** | Int | Invoice identifier |
| **F1_kWh** | Float | kWh of electricity consumed in the F1 time slot |
| **F2_kWh** | Float | kWh of electricity consumed in the F2 time slot |
| **F3_kWh** | Float | kWh of electricity consumed in the F3 time slot  |
| **date** | Date | Start date |
| **light_start_date** | Date | Start date of electricity invoice |
| **light_end_date** | Date | End date of electricity invoice |
| **tv** | Float | Television fee to pay |
| **gas_amount** | Float | Gas fee to pay |
| **gas_average_cost** | Float | Average cost of gas |
| **light_average_cost** | Float | Average cost of electricity |
| **emission_date** | Date | Emission date |
| **supply_type** | String | Supply type (‘light’, ‘gas’, ‘gas and light’) |
| **gas_start_date** | Date | Start date of gas invoice |
| **gas_end_date** | Date | End date of gas invoice |
| **extra_fees** | Float | Extra fees to pay |
| **gas_consumption** | Float | Consumed gas |
| **light_consumption** | Float | Consumed electricity |
| **gas_offer** | Float | Name of the subscribed gas plan (anonymized) |
| **light_offer_type** | String | Kind of plan for the electricity (‘single zone’, ‘bizone’, etc.) |
| **light_offer** | String | Name of the subscribed electricity plan (anonymized) |
| **total_amount** | Float | gas_amount + light_amount + extra_fees |
| **howmuch_pay** | Float | Overall amount to pay, computed as total_amount + tv |
| **light_amount** | Float | Amount to pay for the electricity |
| **average_unit_light_cost** | Float | Average cost for electricity |
| **average_light_bill_cost** | Float | Average cost for the electricity invoice |
| **average_unit_gas_cost** | Float | Average cost for gas |
| **average_gas_bill_cost** | Float | Average cost for the gas invoice |
| **billing_frequency** | String | Billing frequency (‘monthly’, ‘quarterly’, etc.) |
| **bill_type** | String | Kind of invoice (False means a “standard bill”) |
| **gas_system_charges** | Float | Extra gas fees |
| **light_system_charges** | Float | Extra electricity fees |
| **gas_material_cost** | Float | Costs for gas |
| **light_transport_cost** | Float | Extra electricity fees |
| **gas_transport_cost** | Float | Extra gas fees |
| **light_material_cost** | Float | Costs for electricity |
