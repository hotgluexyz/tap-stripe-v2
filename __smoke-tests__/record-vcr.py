from hotglue_smoke_test.vcr.tap import VCRTapTestRunner

from tap_stripe.tap import Tapstripe


class Runner(VCRTapTestRunner):
    PRESERVE_KEYS = {"id", "url", "has_more", "created", "updated", "date"}

    def module(self) -> str:
        return "tap_stripe.tap"

    def launch(self):
        Tapstripe.cli()


if __name__ == "__main__":
    Runner.main()
