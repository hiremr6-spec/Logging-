# Autonomous Travel Booking System
# End-to-end implementation from trigger to ERP sync

import asyncio
import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List


class BookingStatus(Enum):
    PENDING = "pending"
    POLICY_CHECKING = "policy_checking"
    INVENTORY_HOLDING = "inventory_holding"
    PAYMENT_PROCESSING = "payment_processing"
    ACCESS_ISSUING = "access_issuing"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class TravelTriggerType(Enum):
    TRIP_REQUEST = "trip_request"
    EXPENSE_REIMBURSEMENT = "expense_reimbursement"
    AUTO_APPROVED = "auto_approved"


@dataclass
class TravelRequest:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    employee_id: str = ""
    origin: str = ""
    destination: str = ""
    check_in: datetime | None = None
    check_out: datetime | None = None
    budget_limit: float = 0.0
    priority: int = 1
    triggered_by: TravelTriggerType = TravelTriggerType.TRIP_REQUEST

    def __post_init__(self):
        if self.check_in is None:
            self.check_in = datetime.now() + timedelta(days=7)
        if self.check_out is None:
            self.check_out = self.check_in + timedelta(days=3)


@dataclass
class BookingResult:
    booking_id: str
    hotel_id: str
    room_id: str
    total_cost: float
    vcc_number: str = ""
    access_key: str = ""
    status: BookingStatus = BookingStatus.PENDING
    error_message: str = ""


class PolicyEngine:
    """Validates travel against corporate policy."""

    def __init__(self):
        self.policy_rules: Dict[str, Callable[[TravelRequest], bool]] = {
            "budget_cap": self._check_budget_cap,
            "destination_approved": self._check_destination_approved,
            "duration_limit": self._check_duration_limit,
        }
        self.approved_destinations = {"New York", "San Francisco", "London", "Tokyo"}
        self.max_trip_days = 14
        self.category_limits = {
            "executive": 5000.0,
            "senior": 3000.0,
            "standard": 2000.0,
        }

    def _check_budget_cap(self, request: TravelRequest) -> bool:
        return request.budget_limit <= self.category_limits.get("standard", 2000.0)

    def _check_destination_approved(self, request: TravelRequest) -> bool:
        return request.destination in self.approved_destinations

    def _check_duration_limit(self, request: TravelRequest) -> bool:
        duration = (request.check_out - request.check_in).days
        return duration <= self.max_trip_days

    async def verify_policy(self, request: TravelRequest) -> tuple[bool, List[str]]:
        violations: List[str] = []

        for rule_name, rule_func in self.policy_rules.items():
            try:
                if not rule_func(request):
                    violations.append(rule_name)
            except Exception as exc:  # pragma: no cover - defensive guard
                violations.append(f"{rule_name}_error: {str(exc)}")

        return len(violations) == 0, violations


class InventoryAggregator:
    """Manages hotel room inventory with hold/release semantics."""

    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._holds: Dict[str, Dict[str, Any]] = {}
        self._hold_expiry: Dict[str, datetime] = {}

    async def aggregate_availability(self, destination: str, check_in: datetime, check_out: datetime) -> List[Dict]:
        hotels = await self._simulate_pms_query(destination)
        available_rooms: List[Dict] = []

        for hotel in hotels:
            for room in hotel["rooms"]:
                if self._room_available(room, check_in, check_out):
                    available_rooms.append(
                        {
                            "hotel_id": hotel["id"],
                            "hotel_name": hotel["name"],
                            "room_id": room["id"],
                            "room_type": room["type"],
                            "price_per_night": room["price"],
                            "total_cost": room["price"] * (check_out - check_in).days,
                        }
                    )

        return available_rooms

    async def _simulate_pms_query(self, destination: str) -> List[Dict]:
        return [
            {
                "id": f"HTL-{destination[:3].upper()}-001",
                "name": f"{destination} Grand Hotel",
                "rooms": [
                    {"id": "R001", "type": "standard", "price": 200, "available": True},
                    {"id": "R002", "type": "premium", "price": 350, "available": True},
                ],
            },
            {
                "id": f"HTL-{destination[:3].upper()}-002",
                "name": f"{destination} Business Suites",
                "rooms": [
                    {"id": "R101", "type": "standard", "price": 180, "available": True},
                    {"id": "R102", "type": "suite", "price": 450, "available": False},
                ],
            },
        ]

    def _room_available(self, room: Dict, check_in: datetime, check_out: datetime) -> bool:
        return bool(room.get("available", False))

    async def hold_room(self, booking_id: str, room_id: str, expiry_minutes: int = 15) -> bool:
        hold_key = f"{booking_id}:{room_id}"
        expires_at = datetime.now() + timedelta(minutes=expiry_minutes)
        self._holds[hold_key] = {
            "booking_id": booking_id,
            "room_id": room_id,
            "held_at": datetime.now(),
            "expires_at": expires_at,
        }
        self._hold_expiry[hold_key] = expires_at
        return True

    async def release_hold(self, booking_id: str, room_id: str):
        hold_key = f"{booking_id}:{room_id}"
        self._holds.pop(hold_key, None)
        self._hold_expiry.pop(hold_key, None)

    async def confirm_booking(self, booking_id: str, room_id: str) -> bool:
        hold_key = f"{booking_id}:{room_id}"
        if hold_key in self._holds:
            self._cache[room_id] = {
                "status": "booked",
                "booking_id": booking_id,
                "booked_at": datetime.now(),
            }
            await self.release_hold(booking_id, room_id)
            return True
        return False


class BillingOrchestrator:
    """Handles VCC issuance via Stripe/Marqeta."""

    def __init__(self):
        self.vcc_provider = "stripe"
        self._issued_cards: Dict[str, Dict] = {}

    async def mint_vcc(self, booking_id: str, amount: float, merchant_category: str = "hotel") -> Dict[str, str]:
        vcc_number = self._generate_vcc()
        card_data = {
            "vcc_number": vcc_number,
            "amount": amount,
            "merchant_category": merchant_category,
            "issuer": self.vcc_provider,
            "issued_at": datetime.now().isoformat(),
            "status": "active",
            "single_use": True,
            "booking_reference": booking_id,
        }
        self._issued_cards[vcc_number] = card_data

        return {
            "success": True,
            "vcc_number": vcc_number,
            "authorization_code": f"AUTH-{uuid.uuid4().hex[:8].upper()}",
            "settlement_method": "direct_charge",
        }

    def _generate_vcc(self) -> str:
        return f"4242-XXXX-XXXX-{uuid.uuid4().hex[-4:]}"


class AccessService:
    """Issues digital keys for hotel rooms."""

    def __init__(self):
        self._keys: Dict[str, Dict] = {}

    async def issue_key(self, booking_id: str, hotel_id: str, room_id: str) -> Dict[str, Any]:
        access_key = str(uuid.uuid4())
        pin_code = "".join(str(uuid.uuid4().int >> 10 & 0x9) for _ in range(4))

        key_data = {
            "access_key": access_key,
            "pin_code": pin_code,
            "booking_id": booking_id,
            "hotel_id": hotel_id,
            "room_id": room_id,
            "delivery_method": "mobile_ble",
            "valid_from": datetime.now().isoformat(),
            "valid_until": (datetime.now() + timedelta(days=1)).isoformat(),
            "status": "issued",
        }
        self._keys[access_key] = key_data

        return {
            "success": True,
            "access_key": access_key,
            "pin_backup": pin_code,
            "qr_code_url": f"https://key.hotel.api/q/{access_key}",
            "ble_certificate": f"CERT-{access_key[:8]}",
        }


class SagaStep(ABC):
    @abstractmethod
    async def execute(self, context: Dict) -> Dict:
        pass

    @abstractmethod
    async def compensate(self, context: Dict):
        pass


class SagaCoordinator:
    """Orchestrates distributed transactions with compensation."""

    def __init__(self):
        self.steps: List[SagaStep] = []
        self.context: Dict = {}

    async def execute_saga(self, request: TravelRequest) -> BookingResult:
        self.context = {"request": request, "steps_completed": [], "errors": []}

        try:
            result = await self._step_policy_check(request)
            if not result["success"]:
                return await self._fail(result)

            result = await self._step_inventory_hold(request, result)
            if not result["success"]:
                await self._compensate_policy_check(request)
                return await self._fail(result)

            result = await self._step_payment(request, result)
            if not result["success"]:
                await self._compensate_inventory_hold(request, result)
                return await self._fail(result)

            result = await self._step_access(request, result)
            if not result["success"]:
                await self._compensate_payment(request, result)
                await self._compensate_inventory_hold(request, result)
                return await self._fail(result)

            result = await self._step_erp_sync(request, result)
            if not result.status == BookingStatus.COMPLETED:
                await self._compensate_access(request, result.__dict__)
                await self._compensate_payment(request, result.__dict__)
                await self._compensate_inventory_hold(request, result.__dict__)
                return await self._fail({"errors": [result.error_message]})

            return result

        except Exception as exc:
            self.context["errors"].append(str(exc))
            await self._rollback_all()
            return BookingResult(
                booking_id=str(uuid.uuid4()),
                hotel_id="",
                room_id="",
                total_cost=0,
                status=BookingStatus.FAILED,
                error_message=str(exc),
            )

    async def _step_policy_check(self, request: TravelRequest) -> Dict:
        policy_engine = PolicyEngine()
        passed, violations = await policy_engine.verify_policy(request)
        self.context["steps_completed"].append("policy_check")

        if not passed:
            return {"success": False, "errors": violations}
        return {"success": True, "violations": violations}

    async def _step_inventory_hold(self, request: TravelRequest, prev_result: Dict) -> Dict:
        inventory = InventoryAggregator()
        rooms = await inventory.aggregate_availability(request.destination, request.check_in, request.check_out)

        if not rooms:
            return {"success": False, "errors": ["no_rooms_available"]}

        selected = min(rooms, key=lambda room: room["total_cost"])
        booking_id = str(uuid.uuid4())
        await inventory.hold_room(booking_id, selected["room_id"])

        self.context["selected_room"] = selected
        self.context["booking_id"] = booking_id
        self.context["steps_completed"].append("inventory_hold")
        return {"success": True, "room": selected, "booking_id": booking_id}

    async def _step_payment(self, request: TravelRequest, prev_result: Dict) -> Dict:
        billing = BillingOrchestrator()
        room = prev_result["room"]

        vcc_result = await billing.mint_vcc(prev_result["booking_id"], room["total_cost"])

        self.context["vcc"] = vcc_result
        self.context["steps_completed"].append("payment")
        return {"success": True, **prev_result, **vcc_result}

    async def _step_access(self, request: TravelRequest, prev_result: Dict) -> Dict:
        access = AccessService()
        result = await access.issue_key(
            prev_result["booking_id"],
            prev_result["room"]["hotel_id"],
            prev_result["room"]["room_id"],
        )

        self.context["access"] = result
        self.context["steps_completed"].append("access_issuing")
        return {"success": True, **prev_result, **result}

    async def _step_erp_sync(self, request: TravelRequest, prev_result: Dict) -> BookingResult:
        erp_reference = f"ERP-{uuid.uuid4().hex[:8].upper()}"

        self.context["erp_sync"] = {
            "reference": erp_reference,
            "synced_at": datetime.now().isoformat(),
            "ledger_entry": True,
        }
        self.context["steps_completed"].append("erp_sync")

        return BookingResult(
            booking_id=prev_result["booking_id"],
            hotel_id=prev_result["room"]["hotel_id"],
            room_id=prev_result["room"]["room_id"],
            total_cost=prev_result["room"]["total_cost"],
            vcc_number=prev_result.get("vcc_number", ""),
            access_key=prev_result.get("access_key", ""),
            status=BookingStatus.COMPLETED,
        )

    async def _compensate_policy_check(self, request: TravelRequest):
        pass

    async def _compensate_inventory_hold(self, request: TravelRequest, result: Dict):
        if "booking_id" in result and "room" in result:
            inventory = InventoryAggregator()
            await inventory.release_hold(result["booking_id"], result["room"]["room_id"])

    async def _compensate_payment(self, request: TravelRequest, result: Dict):
        if "vcc_number" in result:
            self.context["payment_voided"] = True

    async def _compensate_access(self, request: TravelRequest, result: Dict):
        if "access_key" in result:
            self.context["access_revoked"] = True

    async def _rollback_all(self):
        for step in reversed(self.context.get("steps_completed", [])):
            if step == "inventory_hold":
                await self._compensate_inventory_hold(None, self.context)
            elif step == "payment":
                await self._compensate_payment(None, self.context)
            elif step == "access_issuing":
                await self._compensate_access(None, self.context)

    async def _fail(self, result: Dict) -> BookingResult:
        booking_id = self.context.get("booking_id", str(uuid.uuid4()))
        return BookingResult(
            booking_id=booking_id,
            hotel_id="",
            room_id="",
            total_cost=0,
            status=BookingStatus.FAILED,
            error_message=json.dumps(result.get("errors", [])),
        )


class AutoBookingEngine:
    """Main orchestrator coordinating all components."""

    def __init__(self):
        self.saga = SagaCoordinator()
        self.event_publishers: List[Callable] = []

    async def book_travel(self, request: TravelRequest) -> BookingResult:
        await self._publish_event("booking.triggered", request.__dict__)
        result = await self.saga.execute_saga(request)
        await self._publish_event(
            "booking.completed" if result.status == BookingStatus.COMPLETED else "booking.failed",
            result.__dict__,
        )
        return result

    async def _publish_event(self, event_type: str, payload: Dict):
        for publisher in self.event_publishers:
            await publisher(event_type, payload)


async def main():
    engine = AutoBookingEngine()
    request = TravelRequest(
        employee_id="EMP-12345",
        origin="Boston",
        destination="New York",
        check_in=datetime.now() + timedelta(days=7),
        check_out=datetime.now() + timedelta(days=10),
        budget_limit=1500.0,
        priority=1,
    )

    print(f"Starting booking for {request.employee_id}...")
    print(f"Destination: {request.destination}")
    print(f"Budget limit: ${request.budget_limit}")
    print("-" * 60)

    result = await engine.book_travel(request)

    if result.status == BookingStatus.COMPLETED:
        print("\n✓ Booking Completed!")
        print(f"  Booking ID: {result.booking_id}")
        print(f"  Hotel: {result.hotel_id}")
        print(f"  Room: {result.room_id}")
        print(f"  Total Cost: ${result.total_cost:.2f}")
        print(f"  VCC: {result.vcc_number[:8]}...")
        print(f"  Access Key: {result.access_key[:8]}...")
    else:
        print(f"\n✗ Booking Failed: {result.error_message}")


if __name__ == "__main__":
    asyncio.run(main())
