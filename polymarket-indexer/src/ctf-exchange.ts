import { BigDecimal } from "@graphprotocol/graph-ts"
import { OrderFilled as OrderFilledEvent } from "../generated/CTFExchange/CTFExchange"
import { User, Trade } from "../generated/schema"

export function handleOrderFilled(event: OrderFilledEvent): void {

  // =============================
  // 1️⃣ Handle Maker
  // =============================

  let makerAddress = event.params.maker.toHex()
  let maker = User.load(makerAddress)

  if (maker == null) {
    maker = new User(makerAddress)
    maker.username = makerAddress
    maker.totalTrades = 0
    maker.totalVolume = BigDecimal.fromString("0")
    maker.createdAt = event.block.timestamp
  }

  maker.totalTrades = maker.totalTrades + 1

  // Convert uint256 → BigDecimal (18 decimals assumed)
  let decimals = BigDecimal.fromString("1000000000000000000")

  let makerVolume = event.params.makerAmountFilled
    .toBigDecimal()
    .div(decimals)

  maker.totalVolume = maker.totalVolume.plus(makerVolume)

  maker.save()


  // =============================
  // 2️⃣ Create Trade Entity
  // =============================

  let trade = new Trade(
    event.transaction.hash.toHex() + "-" + event.logIndex.toString()
  )

  trade.user = maker.id
  trade.amount = makerVolume
  trade.timestamp = event.block.timestamp

  trade.save()
}