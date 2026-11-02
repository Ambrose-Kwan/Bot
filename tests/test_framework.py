"""
Unit tests for NexusCheckout framework (Core logic & FSM validation).
Can be run with `pytest` or python's standard `unittest`.
"""
import unittest

from core.exceptions import InvalidStateTransitionError
from core.state_machine import AsyncStateMachine, CheckoutState
from models.profile import ProxyConfig, ShippingAddress


class TestStateMachine(unittest.IsolatedAsyncioTestCase):
    async def test_valid_transitions(self):
        fsm = AsyncStateMachine(task_id="TEST-001", initial_state=CheckoutState.INIT)
        self.assertEqual(fsm.current_state, CheckoutState.INIT)

        await fsm.transition(CheckoutState.MONITOR, reason="Testing monitor transition")
        self.assertEqual(fsm.current_state, CheckoutState.MONITOR)

        await fsm.transition(CheckoutState.ATC, reason="Product found")
        self.assertEqual(fsm.current_state, CheckoutState.ATC)

        await fsm.transition(CheckoutState.SHIPPING)
        self.assertEqual(fsm.current_state, CheckoutState.SHIPPING)

        await fsm.transition(CheckoutState.PAYMENT)
        self.assertEqual(fsm.current_state, CheckoutState.PAYMENT)

        await fsm.transition(CheckoutState.SUCCESS)
        self.assertEqual(fsm.current_state, CheckoutState.SUCCESS)
        self.assertTrue(fsm.is_terminal)

    async def test_invalid_transition_raises_error(self):
        fsm = AsyncStateMachine(task_id="TEST-002", initial_state=CheckoutState.INIT)
        with self.assertRaises(InvalidStateTransitionError):
            # Cannot jump directly from INIT to PAYMENT
            await fsm.transition(CheckoutState.PAYMENT)


class TestModels(unittest.TestCase):
    def test_proxy_parser(self):
        p1 = ProxyConfig.from_string("192.168.1.1:8080:myuser:mypass")
        self.assertEqual(p1.host, "192.168.1.1")
        self.assertEqual(p1.port, 8080)
        self.assertEqual(p1.username, "myuser")
        self.assertEqual(p1.password.get_secret_value(), "mypass")
        self.assertEqual(p1.formatted_url, "http://myuser:mypass@192.168.1.1:8080")

    def test_address_validation(self):
        addr = ShippingAddress(
            first_name="Jane",
            last_name="Doe",
            email="jane.doe@domain.com",
            phone="1234567890",
            address1="100 Main St",
            city="New York",
            state_code="ny",
            postal_code="10001",
            country_code="us",
        )
        self.assertEqual(addr.state_code, "NY")
        self.assertEqual(addr.country_code, "US")
        self.assertEqual(addr.full_name, "Jane Doe")


if __name__ == "__main__":
    unittest.main()
