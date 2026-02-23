import Image from "next/image"

interface HomecastrLogoProps {
    className?: string
    size?: number
    variant?: "icon" | "horizontal"
}

export function HomecastrLogo({ className = "", size = 32, variant = "icon" }: HomecastrLogoProps) {
    if (variant === "horizontal") {
        // Horizontal logo is ~6.4:1 aspect ratio (1050x165)
        const height = size
        const width = Math.round(height * (1050 / 165))
        return (
            <Image
                src="/homecastr-logo-horizontal.png"
                alt="Homecastr"
                width={width}
                height={height}
                className={className}
                style={{ objectFit: "contain" }}
            />
        )
    }
    return (
        <Image
            src="/homecastr-icon.png"
            alt="Homecastr"
            width={size}
            height={size}
            className={className}
            style={{ objectFit: "contain" }}
        />
    )
}
