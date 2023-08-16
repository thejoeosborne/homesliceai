import * as React from "react"
import {
  Box,
  Card,
  CardContent,
  CardHeader,
  Typography,
  Avatar,
  Chip,
  Tooltip,
  IconButton,
  Badge,
} from "@mui/material"
import { TbBed, TbBath } from "react-icons/tb"
import { HiOutlineSquares2X2 } from "react-icons/hi2"
import { MdOutlineKitchen } from "react-icons/md"
import { IoHammerOutline } from "react-icons/io5"
import {
  HiArrowNarrowRight,
  HiOutlineClock,
  HiOutlineHashtag,
} from "react-icons/hi"
import {
  formatCommaInteger,
  formatDateStringMonthDayOnly,
} from "../util/formatters"
import { useCheckIsMobile, useGetWindowSize } from "../util/mobile"
import { formatCurrency } from "../util/formatters"
import PriceChangesDialog from "./PriceChangesDialog"
import { AiFillInfoCircle } from "react-icons/ai"

// Attributes for a listing
interface HomeProfile {
  baths: number
  beds: number
  city: string
  price: number
  price_per_sq_ft: number
  num_kitchens: number
  date_listed: string
  days_on_market: number
  current_days_on_market: number
  images: string[]
  events: any[]
  mls_number: string
  cashflow_amount: number
  seller_motivation: boolean
  seller_motivation_score: string
  sq_ft: number
  state: string
  street_address: string
  url: string
  year_built: number
  zip_code: string
  new: boolean
}

// Color for the badge based on seller motivation score
function getScoreColor(score: string) {
  if (score == "High") {
    return "#81f08c"
  } else if (score == "Moderate") {
    return "#bbedf2"
  } else {
    return "#gray"
  }
}

export default function HomeProfileCard({ data }: { data: HomeProfile }) {
  const isMobile = useCheckIsMobile()
  const windowSize = useGetWindowSize()
  const flexDirection = isMobile ? "column" : "row"
  const [open, setOpen] = React.useState(false)

  return (
    // Wrap everythiing in a badge, display only if the listing is new
    <Badge
      anchorOrigin={{
        vertical: "top",
        horizontal: "left",
      }}
      color="secondary"
      badgeContent={"New"}
      invisible={!data?.new}
      sx={{ display: "flex", flexDirection: "column" }}
    >
      <Card
        sx={{
          display: "flex",
          flexDirection: flexDirection,
          gap: 1,
          boxShadow: 1,
          borderRadius: 2,
          borderWidth: data?.new ? 5 : 1,
          borderColor: data?.new ? "primary.800" : "gray.200",

          // ":hover": {
          //   boxShadow: 4,
          //   cursor: "pointer",
          // },
        }}
        // onClick={() => {
        //   window.open(`https://utahrealestate.com/${data?.mls_number}`)
        // }}
      >
        <CardHeader
          sx={{ padding: 1.5 }}
          avatar={
            // Avatar is used for the home image to nest on the header nicely
            <Avatar
              sx={{
                borderRadius: 1,
                width: isMobile ? windowSize?.width - 60 : 250,
                height: 250,
                mr: "-16px",
              }}
              src={data?.images?.[0]}
            />
          }
        />
        <CardContent
          sx={{
            display: "flex",
            flexDirection: "column",
            justifyContent: "space-between",
          }}
        >
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              justifyContent: "space-between",
              gap: 0.5,
            }}
          >
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                justifyContent: "start",
                alignItems: "center",
                gap: 0.5,
              }}
            >
              <Typography fontSize={24} color="gray.800">
                ${formatCommaInteger(Number(data?.price))}
              </Typography>
              •
              <Typography color="gray.700">
                ${data?.price_per_sq_ft}/sqft
              </Typography>
            </Box>

            <Typography fontSize={14} color="gray.600">
              {data?.street_address}, {data?.city}
            </Typography>
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                gap: 0.5,
                alignItems: "center",
                fontSize: 12,
                //justifyContent: "start",
              }}
            >
              <Typography color="gray.800">{data?.beds}</Typography>
              <TbBed color="gray.600" />•
              <Typography color="gray.800">{data?.baths}</Typography>
              <TbBath color="gray.600" />•
              <Typography color="gray.800">
                {formatCommaInteger(Number(data?.sq_ft))}
              </Typography>
              <HiOutlineSquares2X2 color="gray.600" />•
              <Typography color="gray.800">
                {data?.num_kitchens} Kitchen{data?.num_kitchens != 1 && "s"}
              </Typography>
              <MdOutlineKitchen color="gray.600" />
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                gap: 0.5,
                alignItems: "center",
                //justifyContent: "start",
              }}
            >
              <Typography color="gray.800">{data?.year_built}</Typography>
              <IoHammerOutline color="gray.600" />•
              <Typography color="gray.800">
                {data?.current_days_on_market} days
              </Typography>
              <HiOutlineClock color="gray.600" />
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                gap: 0,
                alignItems: "center",
                //justifyContent: "start",
              }}
            >
              <HiOutlineHashtag size={14} color="gray.600" />
              <Typography fontSize={14} color="gray.600">
                {data?.mls_number}
              </Typography>
            </Box>
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              gap: 1,
              fontSize: 14,
              alignItems: "center",
            }}
          >
            Seller Motivation:
            <Chip
              sx={{
                backgroundColor: getScoreColor(data?.seller_motivation_score),
              }}
              variant="filled"
              size="small"
              label={data?.seller_motivation_score}
            />
            <Box width="50%" sx={{ width: 20, ml: -1 }}>
              <Tooltip
                title={
                  "Seller motivation detection is based on various factors including price, days on market, and AI powered sentiment analysis."
                }
              >
                <IconButton>
                  <AiFillInfoCircle size={18} />
                </IconButton>
              </Tooltip>
            </Box>
          </Box>
          <Box sx={{ display: "flex", flexDirection: "column" }}>
            <Box sx={{ display: "flex", flexDirection: "row", gap: 1 }}>
              <Typography>
                {data?.events?.length} price change
                {data?.events?.length != 1 && "s"}
              </Typography>
              {data?.events?.length > 0 && (
                <Box
                  onClick={() => setOpen(true)}
                  sx={{
                    display: "flex",
                    flexDirection: "row",
                    alignItems: "center",
                    gap: 0.5,
                    color: "primary.800",
                    ":hover": {
                      cursor: "pointer",
                    },
                  }}
                >
                  <Typography color="primary.800">View all</Typography>
                  <Box
                    sx={{
                      display: "flex",
                      pt: 0.25,
                    }}
                  >
                    <HiArrowNarrowRight />
                  </Box>
                </Box>
              )}
            </Box>
            {data?.events?.length > 0 && (
              <Box sx={{ display: "flex", gap: 1 }}>
                <Typography>
                  Latest •{" "}
                  {formatDateStringMonthDayOnly(data?.events?.[0]?.event_date)}{" "}
                  •{" "}
                </Typography>
                <Chip
                  size="small"
                  color="primary"
                  sx={{ color: "gray.900", fontSize: 12, fontWeight: 600 }}
                  label={formatCurrency(Number(data?.events?.[0]?.price_diff))}
                />
              </Box>
            )}
          </Box>
        </CardContent>
        <PriceChangesDialog
          open={open}
          handleClose={() => setOpen(false)}
          data={data?.events}
        />
      </Card>
    </Badge>
  )
}
